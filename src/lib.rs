use std::io;
use std::sync::{self,mpsc};
use std::path;
use std::time;
use std::fs;

trait Queue {
    fn put(&self, bytes: Vec<u8>) -> Result<(), io::Error>;
    fn read_chan(&self) -> mpsc::Receiver<Vec<u8>>;
    fn peek_chan(&self) -> mpsc::Receiver<Vec<u8>>;
    fn close() -> Result<(),()>;
    fn delete() -> Result<(),()>;
    fn depth() -> i64;
    fn empty() -> Result<(),()>;
}

struct DiskQueueState {
    read_pos: i64,
    write_pos: i64,
    read_file_number: i64,
    write_file_number: i64,
    depth: i64,
}

struct DiskQueue {
    state: sync::RwLock<DiskQueueState>,

    name: String,
    data_path: path::PathBuf,
    max_bytes_per_file: i64,
    max_bytes_per_file_read: i64,
    min_msg_size: i32,
    max_msg_size: i32,
    sync_every: i64,
    sync_timeout: time::Duration,
    exit_flag: i32,
    need_sync: bool,

    next_read_pos: i64,
    next_read_file_number: i64,

    read_file: fs::File,
    write_file: fs::File,
    reader: io::BufReader<u8>,
    write_buf: io::BufWriter<u8>,
}