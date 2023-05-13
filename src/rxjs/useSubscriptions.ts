import { useEffect } from "react";
import { Subscription } from "rxjs";

export function useSubscriptions(subscribe:() => Subscription[]) {
  useEffect(() => {
    const subscribtions = subscribe()
    
    return () => {
      for (const subscription of subscribtions) {
        subscription.unsubscribe();
      }
    };
  }, []);
}
