import { Observable } from "rxjs";

export function createAudioPlayer(params: { audioBlob: Blob }) {
  return new Observable<HTMLAudioElement>((subscriber) => {
    let audioUrl = URL.createObjectURL(params.audioBlob);
    let audio = new Audio(audioUrl);
    console.log(`PLAYER - Created for a blob ${params.audioBlob.size}`);

    subscriber.next(audio);

    return () => {
      console.log(`PLAYER - Destroyed for a blob ${params?.audioBlob?.size}`);
      audio.pause();
    };
  });
}
