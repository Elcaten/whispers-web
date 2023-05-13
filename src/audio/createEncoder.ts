import type { Mp3Encoder } from "lamejs";
import { Observable, from, shareReplay, switchMap } from "rxjs";

export const createEncoder = () =>
  from(import("lamejs")).pipe(
    switchMap(({ Mp3Encoder }) => {
      console.log("create encoder");

      return new Observable<Mp3Encoder>((subscriber) => {
        const encoder = new Mp3Encoder(1, 44100, 96);

        subscriber.next(encoder);

        return () => {
          console.log("destroy encoder");
          encoder.flush();
        };
      });
    }),
  );
