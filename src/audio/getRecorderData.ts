import { Observable } from "rxjs";

export function getRecorderData(recorder: MediaRecorder) {
  console.log("create recorder blobs");

  return new Observable<Blob>((subscriber) => {
    const onDataAvailable = (e: BlobEvent) => {
      console.log(e.data.type);
      
      subscriber.next(e.data)
    }
    recorder.addEventListener('dataavailable', onDataAvailable)

    return () => {
      console.log("destroy recorder blobs");
      recorder.removeEventListener("dataavailable", onDataAvailable)
    };
  });
}