import { useEffect } from "react";
import { Subscription } from "rxjs";

/** Runs `subscribe` once on mount.
 * It does not re-run when props/state change;
 * only use for pipelines that intentionally stay fixed for the component lifetime.
 * */
export function useSubscribeOnMount(subscribe: () => Subscription[]) {
  useEffect(() => {
    const subscriptions = subscribe();

    return () => {
      for (const subscription of subscriptions) {
        subscription.unsubscribe();
      }
    };
  }, []);
}
