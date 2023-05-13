import type RecordRTC from "recordrtc";
import type { Options } from "recordrtc";
import { Observable, Subject, from, shareReplay, switchMap } from "rxjs";

export function getRecorderAndBlobs(stream: MediaStream) {
  return from(import("recordrtc")).pipe(
    switchMap(
      ({ default: { RecordRTCPromisesHandler, StereoAudioRecorder } }) => {
        console.log("create recorder");

        const blobs$ = new Subject<Blob>();

        return new Observable<{
          recorder: RecordRTC;
          blobs$: Observable<Blob>;
        }>((subscriber) => {
          const timeSlice = 1000;
          const recorderConfig: Options = {
            mimeType: "audio/wav",
            numberOfAudioChannels: 1,
            recorderType: StereoAudioRecorder,
            sampleRate: 44100,
            timeSlice: timeSlice,
            type: "audio",
            ondataavailable: (blob) => {
              blobs$.next(blob);
            },
          };
          const recorder = new RecordRTCPromisesHandler(stream, recorderConfig);

          subscriber.next({
            recorder,
            blobs$,
          });

          return () => {
            console.log("destroy recorder");
            blobs$.complete();
            recorder.destroy();
          };
        });
      }
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );
}
