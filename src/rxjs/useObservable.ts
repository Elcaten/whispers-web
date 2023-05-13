import { useEffect, useState } from "react";
import { Observable } from "rxjs";

export function useObservable<T>(
  observable: Observable<T>,
  initialValue?: undefined
): T | undefined;
export function useObservable<T>(observable: Observable<T>, initialValue: T): T;
export function useObservable<T>(observable: Observable<T>, initialValue?: T) {
  const [state, setState] = useState<T | undefined>(initialValue);

  useEffect(() => {
    const sub = observable.subscribe(setState);
    return () => sub.unsubscribe();
  }, [observable]);

  return state;
}

