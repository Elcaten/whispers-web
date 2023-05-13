import { Observable } from "rxjs";
import AudioRecorder from 'audio-recorder-polyfill'

export function getRecorder(stream: MediaStream) {
  console.log("create recorder")
  return new Observable<typeof AudioRecorder>((subscriber) => {
    const recorder = new AudioRecorder(stream)

    subscriber.next(recorder)

    return () => {
      console.log("destroy recorder");
      recorder.stop()
    };
  });
}
