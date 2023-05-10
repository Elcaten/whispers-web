import type { Harker } from "hark";
import { Observable, Subject, from, shareReplay, switchMap } from "rxjs";

export type ListenerEvent = "speaking" | "stoppedSpeaking";

export function getListenerAndEvents({ stream }: { stream: MediaStream }) {
  return from(import("hark")).pipe(
    switchMap(({ default: hark }) => {
      console.log("create listener");

      const events$ = new Subject<ListenerEvent>();

      return new Observable<{
        listener: Harker;
        events$: Observable<ListenerEvent>;
      }>((subscriber) => {
        const listener = hark(stream, {
          interval: 100,
          play: false,
          threshold: -70,
        });

        const onSpeaking = () => {
          console.log("speaking");
          events$.next("speaking");
        };
        listener.on("speaking", onSpeaking);

        const onStoppedSpeaking = () => {
          console.log("stopped speaking");
          events$.next("stoppedSpeaking");
        };
        listener.on("stopped_speaking", onStoppedSpeaking);

        subscriber.next({
          listener,
          events$,
        });

        return () => {
          console.log("destroy listener");
          events$.complete();
          listener.off("speaking", onSpeaking);
          listener.off("stopped_speaking", onStoppedSpeaking);
        };
      });
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );
}
