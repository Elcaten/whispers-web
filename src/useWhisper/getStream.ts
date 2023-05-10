import { Observable, from, shareReplay, switchMap } from "rxjs";

export const getStream = () =>
  from(
    navigator.mediaDevices.getUserMedia({
      audio: true,
    })
  ).pipe(
    switchMap((stream) => {
      console.log("create stream");

      return new Observable<MediaStream>((subscriber) => {
        subscriber.next(stream);

        return () => {
          console.log("destroy stream");
          stream.getTracks().forEach((track) => track.stop());
        };
      });
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
