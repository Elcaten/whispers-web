import React, { Fragment, useEffect, useRef, useState } from "react";
import {
  Subject,
  buffer,
  concatMap,
  filter,
  map,
  startWith,
  switchMap,
  withLatestFrom,
} from "rxjs";
import { getEncoder } from "./useWhisper/getEncoder";
import { getListenerAndEvents } from "./useWhisper/getListenerAndEvents";
import { getRecorderAndBlobs } from "./useWhisper/getRecorderAndBlobs";
import { getStream } from "./useWhisper/getStream";
import { getWhisper } from "./useWhisper/getWhisper";
import { getAudioPlayer } from "./utils/getAudioPlayer";
import { shareLatest } from "./utils/shareLatest";

type TranscriptionByDate = Map<Date, string>

export default function App() {
  const [transcriptions, setTranscriptions] = useState<TranscriptionByDate>(new Map());
  const chunks$ = useRef(new Map<Date, Blob>)
  const language$ = useRef(new Subject<"en" | "es">());
  const startRecordingClick$ = useRef(new Subject<void>());
  const stopRecordingClick$ = useRef(new Subject<void>());
  const playClick$ = useRef(new Subject<Date>());

  useEffect(() => {
    const stream$ = startRecordingClick$.current.pipe(
      switchMap(() => getStream()),
      shareLatest()
    );
    const listenerAndEvents$ = stream$.pipe(
      switchMap((stream) => getListenerAndEvents({ stream })),
      shareLatest()
    );
    const listener$ = listenerAndEvents$.pipe(map(({ listener }) => listener));
    const stoppedSpeaking$ = listenerAndEvents$.pipe(
      switchMap(({ events$ }) => events$),
      filter((event) => event === "stoppedSpeaking"),
      shareLatest()
    );
    const recorderWithData$ = stream$.pipe(
      switchMap((stream) => getRecorderAndBlobs(stream)),
      shareLatest()
    );
    const recorder$ = recorderWithData$.pipe(
      map(({ recorder }) => recorder),
      shareLatest()
    );
    const recorderData$ = recorderWithData$.pipe(
      switchMap(({ blobs$ }) => blobs$),
      shareLatest()
    );
    const encoder$ = recorder$.pipe(
      switchMap(() => getEncoder()),
      shareLatest()
    );
    const encodedBlobs$ = recorderData$.pipe(
      withLatestFrom(encoder$),
      concatMap(async ([blob, encoder]) => {
        const buffer = await blob.arrayBuffer();
        const mp3chunk = encoder.encodeBuffer(new Int16Array(buffer));
        const mp3blob = new Blob([mp3chunk], { type: "audio/mpeg" });
        return mp3blob;
      }),
      shareLatest()
    );
    const encodedData$ = encodedBlobs$.pipe(
      buffer(stoppedSpeaking$),
      concatMap(async (encodedBlobs) => {
        const blob = new Blob(encodedBlobs, {
          type: "audio/mpeg",
        });
        const file = new File([blob], "speech.mp3", {
          type: "audio/mpeg",
        });
        return {
          file,
          date: new Date(),
        };
      }),
      shareLatest()
    );

    const transcriptions$ = encodedData$.pipe(
      withLatestFrom(language$.current.pipe(startWith("en" as const))),
      concatMap(([{ file, date }, language]) =>
        getWhisper(file, { language }).then((text) => ({ text, date }))
      ),
      shareLatest()
    );

    const subscriptions = [
      recorder$.subscribe((value) => value.startRecording()),
      stopRecordingClick$.current
        .pipe(withLatestFrom(stream$, recorder$, listener$))
        .subscribe(([_, stream, recorder, listener]) => {
          for (const track of stream.getTracks()) {
            track.stop();
          }
          recorder.stopRecording();
          listener.stop();
        }),
      playClick$.current
        .pipe(
          switchMap((blobDate) => getAudioPlayer({ blob: chunks$.current.get(blobDate)!, contentType: "audio/mpeg" }))
        )
        .subscribe((player) => {
          player.play()
        }),
      encodedData$.subscribe(({ date }) => {
        setTranscriptions((prev) => new Map(prev).set(date, "..."));
      }),
      encodedData$.subscribe(({ file, date }) => {
        chunks$.current = new Map<Date, Blob>(chunks$.current).set(date, file)
      }),
      transcriptions$.subscribe(({ text, date }) => {
        setTranscriptions((prev) => new Map(prev).set(date, text));
      }),
    ];

    return () => {
      for (const s of subscriptions) {
        s.unsubscribe();
      }
    };
  }, []);

  return (
    <div>

      <select onChange={(e) => language$.current.next(e.target.value)} style={{ display: "inline" }}>
        <option value="en">English</option>
        <option value="es">Spanish</option>
      </select>
      <button onClick={() => startRecordingClick$.current.next()}>Start</button>
      <button onClick={() => stopRecordingClick$.current.next()}>Stop</button>
      <pre>
        <code >
          {transcriptions.size === 0 && "Pres start and say something..."}
          {[...transcriptions.keys()].map((date) => (
            <Fragment key={+date}>
              <span style={{ display: "flex", justifyContent: "space-between" }}>
                [{date.toLocaleTimeString()}]: {transcriptions.get(date)} <span style={{ cursor: "pointer" }} onClick={() => playClick$.current.next(date)}>▶️</span>
              </span>
              <br />
            </Fragment>
          ))}
        </code>
      </pre>
    </div>
  );
}


