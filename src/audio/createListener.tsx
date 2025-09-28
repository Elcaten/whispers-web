import type { Harker } from "hark";
import { Observable, from, switchMap } from "rxjs";

export type Listener = {
  stop: () => void;
  setInterval: (value: number) => void;
  setSensetivity: (value: number) => void;
  off: Harker["on"];
} & Pick<Harker, "on">;

export function createListener({
  stream,
  interval,
  sensetivity,
}: {
  stream: MediaStream;
  interval: number;
  sensetivity: number;
}): Observable<Listener> {
  return from(import("hark")).pipe(
    switchMap(({ default: hark }) => {
      return new Observable<Listener>((subscriber) => {
        const listener = hark(stream, {
          interval,
          play: false,
          threshold: -sensetivity,
        });
        console.log(`HARK - Created for a stream ${stream.id}`);

        subscriber.next({
          on: (...args) => listener.on(...(args as Parameters<Listener["on"]>)),
          off: (...args) => listener.off(...(args as Parameters<Listener["off"]>)),
          stop: () => listener.stop(),
          setInterval: (value: number) => listener.setInterval(value),
          setSensetivity: (value: number) => listener.setThreshold(-value),
        });

        return () => {
          listener.stop();
          console.log(`HARK - destroyed for a ${stream.id}`);
        };
      });
    })
  );
}
