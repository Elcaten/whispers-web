import type { Encoder } from "lamejs";
import { Observable, from, shareReplay, switchMap } from "rxjs";

export const getEncoder = () =>
  from(import("lamejs")).pipe(
    switchMap(({ Mp3Encoder }) => {
      console.log("create encoder");

      return new Observable<Encoder>((subscriber) => {
        const encoder = new Mp3Encoder(1, 44100, 96);

        subscriber.next(encoder);

        return () => {
          console.log("destroy encoder");
          encoder.flush();
        };
      });
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
