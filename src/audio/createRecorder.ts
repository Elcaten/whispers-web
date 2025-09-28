import { Observable } from "rxjs";
import AudioRecorder from 'audio-recorder-polyfill'
import mpegEncoder from 'audio-recorder-polyfill/mpeg-encoder'

AudioRecorder.encoder = mpegEncoder
AudioRecorder.prototype.mimeType = 'audio/mpeg'

export function createRecorder(stream: MediaStream) {
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
