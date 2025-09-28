import { useState } from "react";
import { buffer, concatMap, filter, map, merge, mergeMap, of, scan, startWith, switchMap, withLatestFrom } from "rxjs";
import { createAudioPlayer } from "./audio/createAudioPlayer";
import { createListener } from "./audio/createListener";
import { createRecorder } from "./audio/createRecorder";
import { getListenerEvents } from "./audio/getListenerEvents";
import { getRecorderData } from "./audio/getRecorderData";
import { requestAudioStream } from "./audio/requestAudioStream";
import { OPEN_AI_API_KEY } from "./config";
import { fetchTranscription } from "./openAi/fetchTranscription";
import { fetchTranslation } from "./openAi/fetchTranslation";
import { shareLatest } from "./rxjs/shareLatest";
import { useBehaviorSubject } from "./rxjs/useBehaviorSubject";
import { useSubject } from "./rxjs/useSubject";
import { useSubscriptions } from "./rxjs/useSubscriptions";
import { Button } from "./ui/button";
import { Field, Label } from "./ui/fieldset";
import { Input } from "./ui/input";
import { Select } from "./ui/select";
import { Switch, SwitchField } from "./ui/switch";

export default function App() {
  const [interval$, emitInterval, interval] = useBehaviorSubject(50);
  const [sensetivity$, emitSensetivity, sensetivity] = useBehaviorSubject(70);
  const [language$, emitLanguage, language] = useBehaviorSubject<"en" | "es" | "ru">("en");
  const [startRecordingClick$, emitStartRecording] = useSubject<void>();
  const [stopRecordingClick$, emitStopRecording] = useSubject<void>();
  const [playClick$, emitPlayClick] = useSubject<Date>();
  const [generateAudioClick$, emitGenerateAudioClick] = useSubject<Date>();
  const [meSpeaking$, emitMeSpeaking, meSpeaking] = useBehaviorSubject(false);

  const [transcriptionByDate, setTranscriptions] = useState<Map<Date, string>>(new Map());
  const [translationByDate, setTranslations] = useState<Map<Date, string>>(new Map());

  useSubscriptions(() => {
    const audioStream$ = startRecordingClick$.pipe(switchMap(requestAudioStream), shareLatest());

    const listener$ = audioStream$.pipe(
      withLatestFrom(sensetivity$, interval$),
      switchMap(([stream, sensetivity, interval]) => createListener({ stream, sensetivity, interval })),
      shareLatest()
    );
    const stoppedSpeaking$ = listener$.pipe(
      switchMap((listener) => getListenerEvents({ listener })),
      filter((event) => event === "stoppedSpeaking"),
      shareLatest()
    );

    const recorder$ = audioStream$.pipe(
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
        const file = new File(encodedBlobs, `${Date.now()}.mp4`, {
          type: "audio/mp4",
        });
        return {
          file,
          date: new Date(),
        };
      }),
      shareLatest()
    );
    const recoredFileByDate$ = recordedFiles$.pipe(
      scan((acc, { file, date }) => acc.set(date, file), new Map<Date, Blob>()),
      shareLatest()
    );

    const transcriptions$ = recordedFiles$.pipe(
      mergeMap(({ file, date }) =>
        merge(
          of({ text: "...", date }),
          fetchTranscription({ file, apiKey: OPEN_AI_API_KEY }).then((text) => ({ text, date }))
        )
      ),
      shareLatest()
    );

    const transcriptionsByDate$ = transcriptions$.pipe(
      scan((acc, { text, date }) => acc.set(date, text), new Map<Date, string>()),
      shareLatest()
    );

    const translationsByDate$ = transcriptions$.pipe(
      withLatestFrom(language$.pipe(startWith("en" as const))),
      mergeMap(([{ date, text }, language]) =>
        merge(
          of({ text: "...", date }),
          fetchTranslation({ text, targetLanguage: language, apiKey: OPEN_AI_API_KEY }).then((text) => ({ text, date }))
        )
      ),
      scan((acc, { text, date }) => acc.set(date, text), new Map<Date, string>()),
      shareLatest()
    );

    return [
      recorder$.subscribe((value) => value.start(1000)),
      stopRecordingClick$.pipe(withLatestFrom(audioStream$, recorder$, listener$)).subscribe(([_, stream, recorder, listener]) => {
        for (const track of stream.getTracks()) {
          track.stop();
        }
        recorder.stop();
        listener.stop();
      }),

      playClick$
        .pipe(
          withLatestFrom(recoredFileByDate$),
          switchMap(([date, audioFieByDate]) => {
            const audioBlob = audioFieByDate.get(date)!;
            return createAudioPlayer({ audioBlob });
          }),
          switchMap((audioPlayer) => audioPlayer.play())
        )
        .subscribe({
          error: (e) => console.error(e),
          complete: () => console.info("audio playback complete"),
        }),

      generateAudioClick$
        .pipe(
          withLatestFrom(transcriptionsByDate$),
          map(([date, transcriptionsByDate]) => {
            return transcriptionsByDate.get(date)!;
          })
        )
        .subscribe((transcription) => {
          console.log(transcription);
        }),

      transcriptionsByDate$.subscribe((transcriptions) => {
        setTranscriptions(new Map(transcriptions));
      }),
      translationsByDate$.subscribe((translations) => {
        setTranslations(new Map(translations));
      }),

      interval$.pipe(withLatestFrom(listener$)).subscribe(([interval, listener]) => {
        listener.setInterval(interval);
      }),
      sensetivity$.pipe(withLatestFrom(listener$)).subscribe(([sensetivity, listener]) => {
        listener.setSensetivity(sensetivity);
      }),

      meSpeaking$.subscribe((meSpeaking) => {
        console.log(meSpeaking);
      }),
    ];
  });

  return (
    <div className="mt-10 flex flex-col items-center">
      <div className="flex flex-end mb-10 gap-4">
        <Field>
          <Label>Interval</Label>
          <Input type="number" value={interval} onChange={(e) => emitInterval(Number.parseInt(e.target.value))} />
        </Field>
        <Field>
          <Label>Sensetivity</Label>
          <Input type="number" value={sensetivity} onChange={(e) => emitSensetivity(Number.parseInt(e.target.value))} />
        </Field>
        <Field>
          <Label>Translate to</Label>
          <Select value={language} onChange={(e) => emitLanguage(e.target.value as "es" | "en" | "ru")}>
            <option value="en">English</option>
            <option value="es">Spanish</option>
            <option value="ru">Russian</option>
          </Select>
        </Field>
      </div>

      <div className="flex gap-4 mb-4 items-center">
        <Button outline onClick={() => emitStartRecording()}>
          Start
        </Button>
        <Button outline onClick={() => emitStopRecording()}>
          Stop
        </Button>
        <SwitchField>
          <Label>Me speaking</Label>
          <Switch checked={meSpeaking} onChange={(v) => emitMeSpeaking(v)} />
        </SwitchField>
      </div>

      <div className="flex gap-4 p-8">
        <div className="flex flex-col flex-1 gap-2">
          {transcriptionByDate.size === 0 && "Pres start and say something..."}
          {[...transcriptionByDate.keys()].map((date) => (
            <div key={+date} className="bg-gray-100 rounded-md ">
              <span className="flex justify-between gap-1 border-b border-gray-300 py-2 px-4">
                [{date.toLocaleTimeString()}]: {transcriptionByDate.get(date)}{" "}
                <span style={{ cursor: "pointer" }} onClick={() => emitPlayClick(date)}>
                  ▶️
                </span>
              </span>
              <span className="flex justify-between gap-1 py-2 px-4">
                [{date.toLocaleTimeString()}]: {translationByDate.get(date)}
                <span style={{ cursor: "pointer" }} onClick={() => emitGenerateAudioClick(date)}>
                  🔊
                </span>
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
