import useSwrImmutable from "swr/immutable";
import { client } from "./client";

const ELLI_VOICE_ID = "MF3mGyEYCl7XYWbV9V6O"

export function useTextToSpeech({ text /*, voiceId */ }: { text: string/*, voiceId: string */ }) {
  return useSwrImmutable([`/v1/text-to-speech/${ELLI_VOICE_ID}`, text], ([url, text]) => client.post(url, { text }).then(response => new Blob([response.data])))
}
