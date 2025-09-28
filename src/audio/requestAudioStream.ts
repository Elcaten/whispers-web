import { Observable, from, switchMap } from "rxjs";

export const requestAudioStream = (): Observable<MediaStream> =>
  from(
    navigator.mediaDevices.getUserMedia({
      audio: true,
    })
  ).pipe(
    switchMap((stream) => {
      return new Observable<MediaStream>((subscriber) => {
        console.log(`STREAM - Requested stream ${stream.id}`);
        subscriber.next(stream);

        return () => {
          console.log(`STREAM - Destroye stream ${stream.id}`);
          stream.getTracks().forEach((track) => track.stop());
        };
      });
    })
  );
