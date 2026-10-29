#![allow(dead_code)]

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use flate2::read::MultiGzDecoder;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_DECODED_BODY_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn path_and_query(&self) -> (&str, Option<&str>) {
        match self.target.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (&self.target, None),
        }
    }

    pub fn json(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::from_slice(&self.body)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Default)]
struct ClockState {
    now: Duration,
    cancelled: bool,
}

#[derive(Clone, Default)]
pub struct Clock(Arc<(Mutex<ClockState>, Condvar)>);

impl Clock {
    pub fn now(&self) -> Duration {
        self.0.0.lock().expect("clock lock poisoned").now
    }

    pub fn advance(&self, duration: Duration) {
        let mut state = self.0.0.lock().expect("clock lock poisoned");
        state.now = state.now.checked_add(duration).expect("clock overflow");
        self.0.1.notify_all();
    }

    pub fn cancel(&self) {
        self.0.0.lock().expect("clock lock poisoned").cancelled = true;
        self.0.1.notify_all();
    }

    fn wait_until(&self, deadline: Duration, stop: &Receiver<()>) {
        let mut state = self.0.0.lock().expect("clock lock poisoned");
        while state.now < deadline && !state.cancelled {
            if stop.try_recv().is_ok() {
                return;
            }
            state = self
                .0
                .1
                .wait_timeout(state, POLL_INTERVAL)
                .expect("clock lock poisoned")
                .0;
        }
    }
}

pub enum Step {
    Chunk(Vec<u8>),
    AdvanceClock(Clock, Duration),
    WaitUntil(Clock, Duration),
    Signal(Sender<()>),
    Wait(Receiver<()>),
    Disconnect,
}

pub struct Response {
    pub status: String,
    pub headers: Vec<(String, String)>,
    pub steps: Vec<Step>,
}

pub struct HttpContract {
    pub addr: SocketAddr,
    pub requests: Receiver<io::Result<Request>>,
    stop: Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl HttpContract {
    pub fn start(responses: Vec<Response>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let (requests_tx, requests) = mpsc::channel();
        let (stop, stop_rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            for response in responses {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            if stop_rx.recv_timeout(POLL_INTERVAL) != Err(RecvTimeoutError::Timeout)
                            {
                                return;
                            }
                        }
                        Err(error) => {
                            let _ = requests_tx.send(Err(error));
                            return;
                        }
                    }
                };
                if let Err(error) = stream
                    .set_read_timeout(Some(IO_TIMEOUT))
                    .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
                {
                    let _ = requests_tx.send(Err(error));
                    return;
                }
                match read_request(&mut stream) {
                    Ok(request) => {
                        if requests_tx.send(Ok(request)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = requests_tx.send(Err(error));
                        return;
                    }
                }
                if write_response(&mut stream, response, &stop_rx).is_err() {
                    return;
                }
            }
        });
        Ok(Self {
            addr,
            requests,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for HttpContract {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) fn read_request(stream: &mut TcpStream) -> io::Result<Request> {
    let deadline = Instant::now() + IO_TIMEOUT;
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            if pos + 4 > MAX_HEADER_BYTES {
                return Err(invalid_request("request headers too large"));
            }
            break pos + 4;
        }
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers too large",
            ));
        }
        read_more(stream, &mut bytes, deadline)?;
    };
    let header = std::str::from_utf8(&bytes[..header_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut lines = header.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| invalid_request("missing method"))?
        .to_owned();
    let target = request_line
        .next()
        .ok_or_else(|| invalid_request("missing target"))?
        .to_owned();
    let mut headers = Vec::new();
    let mut content_length = None;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| invalid_request("invalid header"))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length: usize = value
                .parse()
                .map_err(|_| invalid_request("invalid content length"))?;
            if content_length.replace(length).is_some() {
                return Err(invalid_request("duplicate content length"));
            }
            if length as u64 > MAX_DECODED_BODY_BYTES {
                return Err(invalid_request("body too large"));
            }
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(invalid_request("transfer encoding is not supported"));
        }
        headers.push((name.to_owned(), value.to_owned()));
    }
    let end = header_end
        .checked_add(content_length.unwrap_or(0))
        .ok_or_else(|| invalid_request("body too large"))?;
    while bytes.len() < end {
        read_more(stream, &mut bytes, deadline)?;
    }
    let mut body = bytes[header_end..end].to_vec();
    for (_, value) in headers
        .iter()
        .rev()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-encoding"))
    {
        for encoding in value.split(',').rev().map(str::trim) {
            let decoder: Box<dyn Read + '_> = if encoding.eq_ignore_ascii_case("gzip") {
                Box::new(MultiGzDecoder::new(body.as_slice()))
            } else if encoding.eq_ignore_ascii_case("zstd") {
                Box::new(zstd::stream::read::Decoder::new(body.as_slice())?)
            } else if encoding.eq_ignore_ascii_case("identity") {
                continue;
            } else {
                return Err(invalid_request("unsupported content encoding"));
            };
            let mut decoded = Vec::new();
            decoder
                .take(MAX_DECODED_BODY_BYTES + 1)
                .read_to_end(&mut decoded)?;
            if decoded.len() as u64 > MAX_DECODED_BODY_BYTES {
                return Err(invalid_request("decoded body too large"));
            }
            body = decoded;
        }
    }
    Ok(Request {
        method,
        target,
        headers,
        body,
    })
}

fn read_more(stream: &mut TcpStream, bytes: &mut Vec<u8>, deadline: Instant) -> io::Result<()> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "request timed out"))?;
    stream.set_read_timeout(Some(remaining))?;
    let mut buffer = [0; 4096];
    match stream.read(&mut buffer)? {
        0 => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete request",
        )),
        count => {
            bytes.extend_from_slice(&buffer[..count]);
            Ok(())
        }
    }
}

fn invalid_request(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn write_response(
    stream: &mut TcpStream,
    response: Response,
    stop: &Receiver<()>,
) -> io::Result<()> {
    write!(stream, "HTTP/1.1 {}\r\n", response.status)?;
    for (name, value) in response.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    stream.write_all(b"Connection: close\r\n\r\n")?;
    for step in response.steps {
        match step {
            Step::Chunk(bytes) => stream.write_all(&bytes)?,
            Step::AdvanceClock(clock, duration) => clock.advance(duration),
            Step::WaitUntil(clock, deadline) => {
                clock.wait_until(deadline, stop);
                return Ok(());
            }
            Step::Signal(reached) => {
                let _ = reached.send(());
            }
            Step::Wait(release) => loop {
                match release.recv_timeout(POLL_INTERVAL) {
                    Ok(()) => break,
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(RecvTimeoutError::Timeout) => {
                        if stop.try_recv().is_ok() {
                            return Ok(());
                        }
                    }
                }
            },
            Step::Disconnect => {
                stream.shutdown(Shutdown::Both)?;
                return Ok(());
            }
        }
    }
    Ok(())
}
