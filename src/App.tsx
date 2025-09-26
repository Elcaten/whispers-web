import { Fragment, useState } from "react";
import { buffer, concatMap, filter, merge, mergeMap, of, scan, startWith, switchMap, withLatestFrom } from "rxjs";
import { createAudioPlayer } from "./audio/createAudioPlayer";
import { createListener } from "./audio/createListener";
import { createMediaStream } from "./audio/createMediaStream";
import { createRecorder } from "./audio/createRecorder";
import { getListenerEvents } from "./audio/getListenerEvents";
import { getRecorderData } from "./audio/getRecorderData";
import { OPEN_AI_API_KEY } from "./config";
import { fetchTranscription } from "./openAi/fetchTranscription";
import { fetchTranslation } from "./openAi/fetchTranslation";
import { shareLatest } from "./rxjs/shareLatest";
import { useBehaviorSubject } from "./rxjs/useBehaviorSubject";
import { useSubject } from "./rxjs/useSubject";
import { useSubscriptions } from "./rxjs/useSubscriptions";

export default function App() {
  const [interval$, emitInterval, interval] = useBehaviorSubject(50);
  const [sensetivity$, emitSensetivity, sensetivity] = useBehaviorSubject(70);
  const [language$, emitLanguage, language] = useBehaviorSubject<"en" | "es" | "ru">("en");
  const [startRecordingClick$, emitStartRecording] = useSubject<void>();
  const [stopRecordingClick$, emitStopRecording] = useSubject<void>();
  const [playClick$, emitPlayClick] = useSubject<Date>();

  const enableTranslation = true;

  const [transcriptionByDate, setTranscriptions] = useState<Map<Date, string>>(new Map());
  const [translationByDate, setTranslations] = useState<Map<Date, string>>(new Map());

  useSubscriptions(() => {
    const stream$ = startRecordingClick$.pipe(
      switchMap(() => createMediaStream()),
      shareLatest()
    );

    const listener$ = stream$.pipe(
      withLatestFrom(sensetivity$, interval$),
      switchMap(([stream, sensetivity, interval]) => createListener({ stream, sensetivity, interval })),
      shareLatest()
    );
    const stoppedSpeaking$ = listener$.pipe(
      switchMap((listener) => getListenerEvents({ listener })),
      filter((event) => event === "stoppedSpeaking"),
      shareLatest()
    );

    const recorder$ = stream$.pipe(
      switchMap((stream) => createRecorder(stream)),
      shareLatest()
    );
    const recordedBlobs$ = recorder$.pipe(
      switchMap((recorder) => getRecorderData(recorder)),
      shareLatest()
    );

    const recordedFiles$ = recordedBlobs$.pipe(
      buffer(stoppedSpeaking$),
      concatMap(async (encodedBlobs) => {
        const file = new File(encodedBlobs, "speech.mp3", {
          type: "audio/mpeg",
        });
        return {
          file,
          date: new Date(),
        };
      }),
      shareLatest()
    );
    const recoredFileByDate$ = recordedFiles$.pipe(scan((acc, { file, date }) => acc.set(date, file), new Map<Date, Blob>()));

    const transcriptions$ = recordedFiles$.pipe(
      withLatestFrom(language$.pipe(startWith("en" as const))),
      mergeMap(([{ file, date }, language]) =>
        merge(
          of({ text: "...", date }),
          fetchTranscription({ file, language, apiKey: OPEN_AI_API_KEY }).then((text) => ({ text, date }))
        )
      ),
      scan((acc, { text, date }) => acc.set(date, text), new Map<Date, string>()),
      shareLatest()
    );

    const translations$ = recordedFiles$.pipe(
      mergeMap(({ file, date }) =>
        merge(
          of({ text: "...", date }),
          fetchTranslation({ file, apiKey: OPEN_AI_API_KEY }).then((text) => ({ text, date }))
        )
      ),
      scan((acc, { text, date }) => acc.set(date, text), new Map<Date, string>()),
      shareLatest()
    );

    return [
      recorder$.subscribe((value) => value.start(1000)),
      stopRecordingClick$.pipe(withLatestFrom(stream$, recorder$, listener$)).subscribe(([_, stream, recorder, listener]) => {
        for (const track of stream.getTracks()) {
          track.stop();
        }
        recorder.stop();
        listener.stop();
      }),

      playClick$
        .pipe(
          withLatestFrom(recoredFileByDate$),
          switchMap(([date, chunkByDate]) =>
            createAudioPlayer({
              blob: chunkByDate.get(date)!,
              contentType: "audio/mpeg",
            })
          )
        )
        .subscribe((player) => {
          player.play();
        }),

      transcriptions$.subscribe((transcriptions) => {
        setTranscriptions(new Map(transcriptions));
      }),
      translations$.subscribe((translations) => {
        setTranslations(new Map(translations));
      }),

      interval$.pipe(withLatestFrom(listener$)).subscribe(([interval, listener]) => {
        listener.setInterval(interval);
      }),
      sensetivity$.pipe(withLatestFrom(listener$)).subscribe(([sensetivity, listener]) => {
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
      <div className="flex" style={{ alignItems: "flex-end", marginBottom: "24px" }}>
        <div>
          Interval:
          <input type="number" value={interval} onChange={(e) => emitInterval(Number.parseInt(e.target.value))} />
        </div>
        <div>
          Sensetivity:
          <input type="number" value={sensetivity} onChange={(e) => emitSensetivity(Number.parseInt(e.target.value))} />
        </div>
        <div style={{ flex: 1 }}>
          Translate to:
          <select value={language} onChange={(e) => emitLanguage(e.target.value as "es" | "en" | "ru")}>
            <option value="en">English</option>
            <option value="es">Spanish</option>
            <option value="ru">Russian</option>
          </select>
        </div>
      </div>

      <div style={{ marginBottom: "16px" }}>
        <button onClick={() => emitStartRecording()}>Start</button>
        <button onClick={() => emitStopRecording()}>Stop</button>
      </div>

      <div style={{ display: "flex", gap: "12px" }}>
        <pre style={{ whiteSpace: "normal", flex: 1 }}>
          <code>
            {transcriptionByDate.size === 0 && "Pres start and say something..."}
            {[...transcriptionByDate.keys()].map((date) => (
              <Fragment key={+date}>
                <span style={{ display: "flex", justifyContent: "space-between" }}>
                  [{date.toLocaleTimeString()}]: {transcriptionByDate.get(date)}{" "}
                  <span style={{ cursor: "pointer" }} onClick={() => emitPlayClick(date)}>
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
