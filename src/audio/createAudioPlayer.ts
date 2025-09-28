import { Observable } from "rxjs";

export function createAudioPlayer(params: { audioBlob: Blob }) {
  return new Observable<HTMLAudioElement>((subscriber) => {
    let audioUrl = URL.createObjectURL(params.audioBlob);
    let audio = new Audio(audioUrl);

    subscriber.next(audio);

    return () => {
      console.log("destroy player for blob", params?.audioBlob?.size);
      audio.pause();
    };
  });
}
