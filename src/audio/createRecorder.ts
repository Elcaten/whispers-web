import AudioRecorder from "audio-recorder-polyfill";
import mpegEncoder from "audio-recorder-polyfill/mpeg-encoder";
import { Observable } from "rxjs";

AudioRecorder.encoder = mpegEncoder;
AudioRecorder.prototype.mimeType = "audio/mpeg";

export function createRecorder(stream: MediaStream): Observable<typeof AudioRecorder> {
  return new Observable<typeof AudioRecorder>((subscriber) => {
    const recorder = new AudioRecorder(stream);
    console.log(`AUIDO_RECORDER - Created for a $tream ${stream.id}`);

    subscriber.next(recorder);

    return () => {
      console.log(`AUDIO_RECORDER - Stopped for a stream ${stream.id}`);
      recorder.stop();
    };
  });
}
