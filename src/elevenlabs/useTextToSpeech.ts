import useSwrImmutable from "swr/immutable";
import { ELEVEN_LABS_VOICE_ID } from "../config";
import { client } from "./client";


export function useTextToSpeech({ text /*, voiceId */ }: { text: string /*, voiceId: string */ }) {
  return useSwrImmutable([`/v1/text-to-speech/${ELEVEN_LABS_VOICE_ID}`, text], ([url, text]) =>
    client.post(url, { text }).then((response) => new Blob([response.data]))
  );
}

export function generateSpeech({ text, voiceId = ELEVEN_LABS_VOICE_ID }: { text: string; voiceId?: string }) {
  return client.post(`/v1/text-to-speech/${voiceId}`, { text, model_id: "eleven_flash_v2_5" }).then((response) => new Blob([response.data]));
}