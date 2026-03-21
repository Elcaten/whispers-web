import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Subject } from "rxjs";

export function useSubject<T>() {
  const subjectRef = useRef(new Subject<T>());
  const stream$ = useMemo(() => subjectRef.current.asObservable(), []);
  const setValue = useCallback((value: T) => subjectRef.current.next(value), []);

  return [stream$, setValue] as const;
}
