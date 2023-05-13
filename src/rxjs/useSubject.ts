import { useRef, useCallback } from "react";
import { Subject } from "rxjs";
import { useObservable } from "./useObservable";

export function useSubject<T>() {
  const subject = useRef(new Subject<T>());
  const value = useObservable(subject.current);
  const setValue = useCallback(
    (value: T) => subject.current.next(value),
    []
  );
  return [subject.current.asObservable(), setValue, value] as const;
}
