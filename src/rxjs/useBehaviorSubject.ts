import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BehaviorSubject } from "rxjs";

export function useBehaviorSubject<T>(initialValue: T) {
  const subject$ = useRef(new BehaviorSubject<T>(initialValue));
  const [subjectState, setSubjectState] = useState<T | undefined>(initialValue);
  const stream$ = useMemo(() => subject$.current.asObservable(), []);
  const setValue = useCallback((value: T) => subject$.current.next(value), []);

  useEffect(() => {
    const sub = subject$.current.subscribe(setSubjectState);
    return () => sub.unsubscribe();
  }, [subject$.current]);

  return [stream$, setValue, subjectState] as const;
}
