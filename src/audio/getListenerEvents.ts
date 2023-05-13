import { Observable } from "rxjs";
import { Listener } from "./createListener";

export type ListenerEvent = "speaking" | "stoppedSpeaking";

export function getListenerEvents({ listener }: { listener: Listener }) {
  console.log("create listener events");

  return new Observable<ListenerEvent>((subscriber) => {
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
      console.log("stopped speaking");
      subscriber.next("stoppedSpeaking");
    };
    listener.on("stopped_speaking", onStoppedSpeaking);

    return () => {
      console.log("destroy listener events");
      listener.off("speaking", onSpeaking);
      // listener.off("volume_change", onVolumeChange);
      listener.off("stopped_speaking", onStoppedSpeaking);
    };
  });
}
