import { Observable } from "rxjs";

export function createRecorder(stream: MediaStream) {
  console.log("create recorder")
  return new Observable<MediaRecorder>((subscriber) => {
    const recorder = new MediaRecorder(stream, {
      mimeType: 'audio/mp4'
    })

    subscriber.next(recorder)

    return () => {
      console.log("destroy recorder");
      recorder.stop()
    };
  });
}
