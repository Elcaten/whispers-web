import { useCallback, useRef } from "react";
import { BehaviorSubject } from "rxjs";
import { useObservable } from "./useObservable";

export function useBehaviorSubject<T>(initialValue: T) {
  const subject$ = useRef(new BehaviorSubject<T>(initialValue));
  const value = useObservable(subject$.current, initialValue);
  const setValue = useCallback(
    (value: T) => subject$.current.next(value),
    []
  );
  return [subject$.current.asObservable(), setValue, value] as const;
}
