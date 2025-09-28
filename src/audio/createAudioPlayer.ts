import { Howl } from "howler";
import { Observable, from, switchMap } from "rxjs";

async function getHowler(params: { blob: Blob; contentType: string }) {
  const sound = new Howl({
    src: [await blobToBase64(params.blob)],
  });
  console.log("new Howl ", params.blob.size);
  return {
    play: () => sound.play(),
    unload: () => sound.unload(),
  };
}

function blobToBase64(blob: Blob) {
  return new Promise<string>((resolve) => {
    const reader = new FileReader();
    reader.onloadend = () => {
      console.log("read blob finish", blob.size);
      return resolve(reader.result as string);
    };
    reader.readAsDataURL(blob);
  });
}

export function createAudioPlayer(params: { blob: Blob; contentType: string }) {
  return from(getHowler(params)).pipe(
    switchMap((sound) => {
      console.log("created player for blob", params.blob.size);

      return new Observable<{ play: () => void }>((subscriber) => {
        subscriber.next({
          play: () => {
            console.log("play blob", params.blob.size);
            return sound.play();
          },
        });

        return () => {
          console.log("destroy player for blob", params.blob.size);
          sound.unload();
        };
      });
    })
  );
}