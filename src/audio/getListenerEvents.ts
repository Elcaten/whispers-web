import { Observable } from "rxjs";
import { Listener } from "./createListener";

export type ListenerEvent = "speaking" | "stoppedSpeaking";

export function getListenerEvents({ listener }: { listener: Listener }) {
  return new Observable<ListenerEvent>((subscriber) => {
    console.log(`HARK_EVENTS - created`);
    const onSpeaking = () => {
      // console.log("speaking");
      subscriber.next("speaking");
    };
    listener.on("speaking", onSpeaking);

    // const onVolumeChange = (val: any) => {
    //   console.log("volume_change: " + val);
    // };
    // listener.on("volume_change", onVolumeChange);

    const onStoppedSpeaking = () => {
      console.log(`HARK_EVENTS - Stopped Speaking`);
      subscriber.next("stoppedSpeaking");
    };
    listener.on("stopped_speaking", onStoppedSpeaking);

    return () => {
      console.log(`HARK_EVENTS - Destroyed`);
      listener.off("speaking", onSpeaking);
      // listener.off("volume_change", onVolumeChange);
      listener.off("stopped_speaking", onStoppedSpeaking);
    };
  });
}
