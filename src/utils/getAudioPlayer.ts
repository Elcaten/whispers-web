import { Howl } from "howler";
import { Observable, from, shareReplay, switchMap } from "rxjs";

async function getHowler(params: { blob: Blob, contentType: string }) {
  const sound = new Howl({
    src: [`data:${params.contentType};base64,${await blobToBase64(params.blob)}`],
  });
  return {
    play: () => sound.play(),
    unload: () => sound.unload
  }
}

function blobToBase64(blob: Blob) {
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onloadend = () => resolve((reader.result as string).split(",")[1]);
    reader.readAsDataURL(blob);
  });
}

export function getAudioPlayer(params: { blob: Blob, contentType: string }) {
  return from(
    getHowler(params)
  ).pipe(
    switchMap((sound) => {
      console.log("playing blob");

      return new Observable<{ play: () => void }>((subscriber) => {
        subscriber.next({ play: () => sound.play() });

        return () => {
          console.log("stop playing blob");
          sound.unload()
        };
      });
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );

}