use axum::{
    body::{Body, Bytes},
    debug_handler,
    extract::{FromRef, Path, State},
    http::{header::TRANSFER_ENCODING, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, put},
    Error, Router,
};

use core::pin::Pin;
use futures_core::{stream, task::Waker};
use futures_core::{
    task::{Context, Poll},
    Stream,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type AppState = State<MyState>;

const BYTES_PER_CHUNK: usize = 8;
const WRITE_BUFFER_CAPACITY: usize = 100; //in bytes

async fn hello_world<'a>() -> &'a str {
    return "hi!";
}

async fn delete_stream(State(state): AppState, Path(id): Path<usize>) -> impl IntoResponse {
    let mut stream_map = state.connections.lock().await;
    let valid_id = stream_map.contains_key(&id);
    if valid_id {
        stream_map.remove(&id);
        return Ok((StatusCode::NO_CONTENT, "File succesfully deleted!"));
    } else {
        return Err((StatusCode::NOT_FOUND, "Requested stream ID was not found!"));
    }
}
async fn id_exists(state: &MyState, id: usize) -> bool {
    let stream_map = state.connections.lock().await;
    return stream_map.contains_key(&id);
}

async fn manage_readers(stream: &StreamCon) {
    let readers = &mut stream.readers.lock().unwrap();
    for reader in readers.iter() {
        reader.clone().wake();
    }
    readers.clear();
}

async fn accept_stream(
    State(state): AppState,
    Path(id): Path<usize>,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    if id_exists(&state, id).await {
        let mut stream_map = state.connections.lock().await;
        stream_map.remove(&id);
    }

    let chunked_encoding =
        headers.contains_key(TRANSFER_ENCODING) && headers[TRANSFER_ENCODING] == "chunked";
    let mut buffer = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
    let iters = payload.len() / BYTES_PER_CHUNK;

    for i in 0..iters + 1 {
        let (payload_start, payload_end) = (
            i * BYTES_PER_CHUNK,
            std::cmp::min((i + 1) * BYTES_PER_CHUNK, iters),
        );

        //measure time, maybe Bytes is smart enough to not copy to writer and just reference it (should be)
        buffer.push(Bytes::copy_from_slice(&payload[payload_start..payload_end]));

        let end_of_request = (i + 1) * BYTES_PER_CHUNK >= iters;
        if buffer.len() > WRITE_BUFFER_CAPACITY || end_of_request {
            {
                let mut stream_map = state.connections.lock().await;
                let stream = stream_map.entry(id).or_default();

                let writer = &mut stream.writer.write().await;
                writer.data.append(&mut buffer);
                buffer.clear();

                let chunked_transfer_end = chunked_encoding && payload.len() == 0;
                let nonchunked_req_end = !chunked_encoding && (i + 1) * BYTES_PER_CHUNK >= iters;

                if chunked_transfer_end || nonchunked_req_end {
                    writer.ended = true;
                }

                manage_readers(stream).await;
            }
        }
    }
    return Ok((StatusCode::ACCEPTED, "Chunk finished!".to_owned()));
}

async fn return_stream(State(state): AppState, Path(id): Path<usize>) -> impl IntoResponse {
    let stream_map = state.connections.lock().await;

    if let Some(stream) = stream_map.get(&id) {
        let cache_stream = CacheStream {
            cache: stream.writer.clone(),
            wakers: stream.readers.clone(),
            index: 0,
        };
        let body = Body::from_stream(cache_stream);

        return Ok((StatusCode::ACCEPTED, body));
    } else {
        return Err((StatusCode::NOT_FOUND, "Requested stream ID was not found!"));
    }
}

#[tokio::main]
async fn main() {
    assert!(BYTES_PER_CHUNK < WRITE_BUFFER_CAPACITY);
    let state = MyState::default();

    let app = Router::new()
        .route("/", get(hello_world))
        .route("/upload/:id", put(accept_stream))
        .route("/delete/:id", delete(delete_stream))
        .route("/download/:id", get(return_stream))
        .with_state(state);
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Default, Clone, Debug, FromRef)]
struct MyState {
    connections: Arc<tokio::sync::Mutex<HashMap<usize, StreamCon>>>,
}

#[derive(Clone, Default, Debug)]
struct StreamCon {
    readers: Arc<std::sync::Mutex<Vec<Waker>>>,
    writer: Arc<tokio::sync::RwLock<Writer>>,
}

#[derive(Clone, Default, Debug)]
struct Writer {
    ended: bool,
    data: Vec<Bytes>,
}

struct CacheStream {
    cache: Arc<tokio::sync::RwLock<Writer>>,
    wakers: Arc<std::sync::Mutex<Vec<Waker>>>,
    index: usize,
}

impl Stream for CacheStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, move_index) = match self.cache.try_read() {
            Ok(cache) => {
                if cache.ended == true && self.index == cache.data.len() {
                    (Poll::Ready(None), false)
                } else if self.index == cache.data.len() {
                    self.wakers.lock().unwrap().push(cx.waker().clone());
                    (Poll::Pending, false)
                } else {
                    (Poll::Ready(Some(Ok(cache.data[self.index].clone()))), true)
                }
            }
            Err(_e) => (Poll::Pending, false),
        };
        if move_index {
            self.get_mut().index += 1;
        }
        return result;
    }
}
