import { shareReplay } from "rxjs";

/** Multicast + replay last value. `refCount` tears down the source and drops the buffer when the last subscriber unsubscribes; new subscribers do not replay until the source emits again. */
export function shareLatest<T>() {
  return shareReplay<T>({ bufferSize: 1, refCount: true });
}
