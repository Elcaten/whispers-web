import { ChevronDownIcon, CogIcon, LanguageIcon, PlayPauseIcon } from "@heroicons/react/16/solid";
import { Fragment, useState } from "react";
import { buffer, concatMap, filter, map, merge, mergeMap, Observable, scan, switchMap, withLatestFrom } from "rxjs";
import { createAudioPlayer } from "./audio/createAudioPlayer";
import { createListener } from "./audio/createListener";
import { createRecorder } from "./audio/createRecorder";
import { getListenerEvents } from "./audio/getListenerEvents";
import { getRecorderData } from "./audio/getRecorderData";
import { requestAudioStream } from "./audio/requestAudioStream";
import { OPEN_AI_API_KEY } from "./config";
import { generateSpeech } from "./elevenlabs/useTextToSpeech";
import { fetchTranscription, VerboseJsonResponse } from "./openAi/fetchTranscription";
import { fetchTranslation } from "./openAi/fetchTranslation";
import { shareLatest } from "./rxjs/shareLatest";
import { useBehaviorSubject } from "./rxjs/useBehaviorSubject";
import { useSubject } from "./rxjs/useSubject";
import { useSubscriptions } from "./rxjs/useSubscriptions";
import { Button } from "./ui/button";
import { Dialog, DialogActions, DialogBody, DialogDescription, DialogTitle } from "./ui/dialog";
import { Field, FieldGroup, Fieldset, Label } from "./ui/fieldset";
import { Input } from "./ui/input";
import { Navbar, NavbarItem, NavbarLabel, NavbarSection, NavbarSpacer } from "./ui/navbar";
import { Select } from "./ui/select";

export default function App() {
  const [interval$, emitInterval, interval] = useBehaviorSubject(50);
  const [sensetivity$, emitSensetivity, sensetivity] = useBehaviorSubject(70);
  const [yourLang$, emitYourLang, yourLang] = useBehaviorSubject<"english" | "russian">("english");
  const [theirLang$, emitTheirLang, theirLang] = useBehaviorSubject<"serbian" | "spanish">("spanish");
  const [startRecordingClick$, emitStartRecording] = useSubject<void>();
  const [stopRecordingClick$, emitStopRecording] = useSubject<void>();
  const [playClick$, emitPlayClick] = useSubject<Date>();
  const [generateAudioClick$, emitGenerateAudioClick] = useSubject<Date>();

  const [transcriptionByDate, setTranscriptions] = useState<Map<Date, VerboseJsonResponse>>(new Map());
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

    const transcriptions$: Observable<{
      verboseJson: VerboseJsonResponse;
      date: Date;
    }> = recordedFiles$.pipe(
      mergeMap(({ file, date }) =>
        merge(
          // of({ text: { text: "...", task: "", lan }, date }),
          fetchTranscription({ file, apiKey: OPEN_AI_API_KEY, response_format: "verbose_json" }).then((verboseJson) => ({ verboseJson, date }))
        )
      ),
      shareLatest()
    );

    const transcriptionsByDate$ = transcriptions$.pipe(
      scan((acc, { verboseJson, date }) => acc.set(date, verboseJson), new Map<Date, VerboseJsonResponse>()),
      shareLatest()
    );

    const translationsByDate$ = transcriptions$.pipe(
      withLatestFrom(yourLang$),
      mergeMap(([{ date, verboseJson }, yourLang]) => {
        const isTheirLang = verboseJson.language !== yourLang;
        const translateTo = isTheirLang ? yourLang : theirLang;
        return merge(
          // of({ text: "...", date }),
          fetchTranslation({ text: verboseJson.text, targetLanguage: translateTo, apiKey: OPEN_AI_API_KEY }).then((text) => ({ text, date }))
        );
      }),
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
          withLatestFrom(translationsByDate$),
          map(([date, translationsByDate]) => {
            return translationsByDate.get(date)!;
          }),
          switchMap((text) => generateSpeech({ text })),
          switchMap((audioBlob) => createAudioPlayer({ audioBlob })),
          switchMap((player) => player.play())
        )
        .subscribe({
          error: (e) => console.error(e),
          complete: () => console.info("audio playback complete"),
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
    ];
  });

  let [isOpen, setIsOpen] = useState(false);

  return (
    <Fragment>
      <div>
        <Navbar className="border border-zinc-200 px-4 sticky top-0 bg-white z-2">
          <NavbarItem onClick={() => setIsOpen(true)}>
            <CogIcon />
            <NavbarLabel>Settings</NavbarLabel>
            <ChevronDownIcon />
          </NavbarItem>

          <NavbarSpacer />
          <NavbarSection>
            <NavbarItem onClick={() => emitStartRecording()}>Start</NavbarItem>
          </NavbarSection>
          <NavbarSection>
            <NavbarItem onClick={() => emitStopRecording()}>Stop</NavbarItem>
          </NavbarSection>
        </Navbar>

        <div className="mt-10 flex flex-col items-center z-1 m-auto max-w-2xl">
          <div className="flex gap-4 p-8 w-full">
            <div className="flex flex-col flex-1 gap-2">
              {transcriptionByDate.size === 0 && (
                <span className="text-center">
                  Press
                  <Button className="mx-2" color="green" onClick={() => emitStartRecording()}>
                    Start
                  </Button>
                  and say something...
                </span>
              )}
              {[...transcriptionByDate.keys()].map((date) => {
                const isTheirLang = transcriptionByDate.get(date)?.language !== yourLang;
                return (
                  <div key={+date} className={`bg-gray-100 rounded-xl ${isTheirLang ? "mr-32" : "ml-32"}`}>
                    <span>
                      <span className="flex justify-between items-center gap-4 border-b border-gray-300 py-1 px-6">
                        <span> {transcriptionByDate.get(date)?.text ?? "???"} </span>
                        <Button plain onClick={() => emitPlayClick(date)}>
                          <PlayPauseIcon />
                        </Button>
                      </span>
                      <span className="flex justify-between gap-4 py-1 px-6 items-center ">
                        <span> {translationByDate.get(date)} </span>
                        <Button plain onClick={() => emitGenerateAudioClick(date)}>
                          <LanguageIcon />
                        </Button>
                      </span>
                    </span>
                    <span className="px-2 py-1 text-gray-400 text-xs flex justify-end">{date.toLocaleTimeString()}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      <Dialog open={isOpen} onClose={setIsOpen}>
        <DialogTitle>Settings</DialogTitle>
        <DialogDescription>The refund will be reflected in the customer’s bank account 2 to 3 business days after processing.</DialogDescription>
        <DialogBody>
          <Fieldset>
            <FieldGroup>
              <Field>
                <Label>Interval {interval}</Label>
                <Input type="range" value={interval} onChange={(e) => emitInterval(Number.parseInt(e.target.value))} />
              </Field>
              <Field>
                <Label>Sensetivity {sensetivity}</Label>
                <Input type="range" value={sensetivity} onChange={(e) => emitSensetivity(Number.parseInt(e.target.value))} />
              </Field>
              <Field>
                <Label>You speak</Label>
                <Select value={yourLang} onChange={(e) => emitYourLang(e.target.value as "english" | "russian")}>
                  <option value="english">English</option>
                  <option value="russian">Russian</option>
                </Select>
              </Field>
              <Field>
                <Label>They speak</Label>
                <Select value={theirLang} onChange={(e) => emitTheirLang(e.target.value as "spanish" | "serbian")}>
                  <option value="spanish">Spanish</option>
                  <option value="serbian">Serbian</option>
                </Select>
              </Field>
            </FieldGroup>
          </Fieldset>
        </DialogBody>
        <DialogActions>
          <Button color="red" onClick={() => setIsOpen(false)}>
            Cancel
          </Button>
          <Button color="green" onClick={() => setIsOpen(false)}>
            Confirm
          </Button>
        </DialogActions>
      </Dialog>
    </Fragment>
  );
}
