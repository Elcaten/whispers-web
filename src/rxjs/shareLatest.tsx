import { shareReplay } from "rxjs";

export function shareLatest<T>() {
  return shareReplay<T>({ bufferSize: 1, refCount: true });
}
