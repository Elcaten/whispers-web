import { Observable } from "rxjs";

export function getRecorderData(recorder: MediaRecorder) {
  return new Observable<Blob>((subscriber) => {
    console.log("RECORDER_BLOBS - Created");

    const onDataAvailable = (e: BlobEvent) => {
      console.log(`RECODER_EVENTS - New ${e.data.type} data available `);
      subscriber.next(e.data);
    };
    recorder.addEventListener("dataavailable", onDataAvailable);

    return () => {
      console.log(`RECORDER_BLOBS - Destroyed`);
      recorder.removeEventListener("dataavailable", onDataAvailable);
    };
  });
}