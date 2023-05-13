import React, { Fragment, useState } from "react";
import {
  buffer,
  concatMap,
  filter,
  map,
  mergeMap,
  repeat,
  scan,
  startWith,
  switchMap,
  takeUntil,
  withLatestFrom,
} from "rxjs";
import { createEncoder } from "./audio/createEncoder";
import { fetchTranscription } from "./openAi/fetchTranscription";
import { fetchTranslation } from "./openAi/fetchTranslation";
import { getAudioPlayer } from "./audio/getAudioPlayer";
import { shareLatest } from "./rxjs/shareLatest";
import { useBehaviorSubject } from "./rxjs/useBehaviorSubject";
import { useSubject } from "./rxjs/useSubject";
import { useSubscriptions } from "./rxjs/useSubscriptions";
import { getListenerEvents } from "./audio/getListenerEvents";
import { createListener } from "./audio/createListener";
import { createMediaStream } from "./audio/createMediaStream";
import { getRecorderAndBlobs } from "./audio/getRecorderAndBlobs";
import { useObservable } from "./rxjs/useObservable";
import { API_KEY } from "./config";

export default function App() {
  const [interval$, emitInterval, interval] = useBehaviorSubject(50);
  const [sensetivity$, emitSensetivity, sensetivity] = useBehaviorSubject(70);
  const [language$, emitLanguage, language] = useBehaviorSubject<"en" | "es">(
    "es"
  );
  const [startRecordingClick$, emitStartRecording] = useSubject<void>();
  const [stopRecordingClick$, emitStopRecording] = useSubject<void>();
  const [playClick$, emitPlayClick] = useSubject<Date>();

  const enableTranslation$ = language$.pipe(
    map((language) => language !== "en"),
    shareLatest()
  );
  const enableTranslation = useObservable(enableTranslation$)

  const [transcriptionByDate, setTranscriptions] = useState<Map<Date, string>>(
    new Map()
  );
  const [translationByDate, setTranslations] = useState<Map<Date, string>>(
    new Map()
  );

  useSubscriptions(() => {
    const stream$ = startRecordingClick$.pipe(
      switchMap(() => createMediaStream()),
      shareLatest()
    );

    const listener$ = stream$.pipe(
      withLatestFrom(sensetivity$, interval$),
      switchMap(([stream, sensetivity, interval]) =>
        createListener({ stream, sensetivity, interval })
      ),
      shareLatest()
    );
    const stoppedSpeaking$ = listener$.pipe(
      switchMap((listener) => getListenerEvents({ listener })),
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
      switchMap(() => createEncoder()),
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
    const chunkByDate$ = encodedData$.pipe(
      scan((acc, { file, date }) => acc.set(date, file), new Map<Date, Blob>())
    );

    const transcriptions$ = encodedData$.pipe(
      withLatestFrom(language$.pipe(startWith("en" as const))),
      mergeMap(([{ file, date }, language]) =>
        fetchTranscription({ file, language, apiKey: API_KEY }).then((text) => ({ text, date }))
      ),
      scan(
        (acc, { text, date }) => acc.set(date, text),
        new Map<Date, string>()
      ),
      shareLatest()
    );

    const translations$ = encodedData$.pipe(
      withLatestFrom(enableTranslation$),
      filter(([_, enableTranslation]) => enableTranslation),
      mergeMap(([{ file, date }]) =>
        fetchTranslation({ file, apiKey: API_KEY }).then((text) => ({ text, date }))
      ),
      scan(
        (acc, { text, date }) => acc.set(date, text),
        new Map<Date, string>()
      ),
      shareLatest()
    );

    return [
      recorder$.subscribe((value) => value.startRecording()),
      stopRecordingClick$
        .pipe(withLatestFrom(stream$, recorder$, listener$))
        .subscribe(([_, stream, recorder, listener]) => {
          for (const track of stream.getTracks()) {
            track.stop();
          }
          recorder.stopRecording();
          listener.stop();
        }),

      playClick$
        .pipe(
          withLatestFrom(chunkByDate$),
          switchMap(([date, chunkByDate]) =>
            getAudioPlayer({
              blob: chunkByDate.get(date)!,
              contentType: "audio/mpeg",
            })
          )
        )
        .subscribe((player) => {
          player.play();
        }),

      encodedData$.subscribe(({ date }) => {
        setTranscriptions((prev) => new Map(prev).set(date, "..."));
        setTranslations((prev) => new Map(prev).set(date, "..."));
      }),
      transcriptions$.subscribe((transcriptions) => {
        setTranscriptions(new Map(transcriptions));
      }),
      translations$.subscribe((translations) => {
        setTranscriptions(new Map(translations));
      }),

      interval$
        .pipe(withLatestFrom(listener$))
        .subscribe(([interval, listener]) => {
          listener.setInterval(interval);
        }),
      sensetivity$
        .pipe(withLatestFrom(listener$))
        .subscribe(([sensetivity, listener]) => {
          listener.setSensetivity(sensetivity);
        }),
    ];
  });

  return (
    <div
      style={{
        marginTop: "40px",
        display: "flex",
        flexFlow: "column",
        alignItems: "center",
      }}
    >
      <flex style={{ alignItems: "flex-end", marginBottom: "24px" }}>
        <div>
          Interval:
          <input
            type="number"
            value={interval}
            onChange={(e) => emitInterval(Number.parseInt(e.target.value))}
          />
        </div>
        <div>
          Sensetivity:
          <input
            type="number"
            value={sensetivity}
            onChange={(e) => emitSensetivity(Number.parseInt(e.target.value))}
          />
        </div>
        <div style={{ flex: 1 }}>
          Language:
          <select
            value={language}
            onChange={(e) => emitLanguage(e.target.value as "es" | "en")}
          >
            <option value="en">English</option>
            <option value="es">Spanish</option>
          </select>
        </div>
      </flex>

      <div style={{ marginBottom: "16px" }}>
        <button onClick={() => emitStartRecording()}>Start</button>
        <button onClick={() => emitStopRecording()}>Stop</button>
      </div>

      <div style={{ display: "flex", gap: "12px" }}>
        <pre style={{ whiteSpace: "normal", flex: 1 }}>
          <code>
            {transcriptionByDate.size === 0 &&
              "Pres start and say something..."}
            {[...transcriptionByDate.keys()].map((date) => (
              <Fragment key={+date}>
                <span
                  style={{ display: "flex", justifyContent: "space-between" }}
                >
                  [{date.toLocaleTimeString()}]: {transcriptionByDate.get(date)}{" "}
                  <span
                    style={{ cursor: "pointer" }}
                    onClick={() => emitPlayClick(date)}
                  >
                    ▶️
                  </span>
                </span>
                <br />
              </Fragment>
            ))}
          </code>
        </pre>
        <pre
          style={{
            opacity: enableTranslation ? 1 : 0.25,
            whiteSpace: "normal",
            flex: 1,
          }}
        >
          <code>
            {translationByDate.size === 0 && "Translating..."}
            {[...translationByDate.keys()].map((date) => (
              <Fragment key={+date}>
                <div>
                  [{date.toLocaleTimeString()}]: {translationByDate.get(date)}
                </div>
                <br />
              </Fragment>
            ))}
          </code>
        </pre>
      </div>
    </div>
  );
}
