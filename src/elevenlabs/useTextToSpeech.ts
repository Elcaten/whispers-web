import { useState, useEffect } from "react";
import { ELEVEN_LABS_VOICE_ID } from "../config";
import { client } from "./client";

export function useTextToSpeech({ text /*, voiceId */ }: { text: string /*, voiceId: string */ }) {
  const [data, setData] = useState<Blob | undefined>(undefined);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!text) return;
    
    setIsLoading(true);
    setError(undefined);
    
    client.post(`/v1/text-to-speech/${ELEVEN_LABS_VOICE_ID}`, { text })
      .then((response) => new Blob([response.data]))
      .then(setData)
      .catch(setError)
      .finally(() => setIsLoading(false));
  }, [text]);

  return { data, error, isLoading };
}

export function generateSpeech({ text, voiceId = ELEVEN_LABS_VOICE_ID }: { text: string; voiceId?: string }) {
  return client.post(`/v1/text-to-speech/${voiceId}`, { text, model_id: "eleven_flash_v2_5" }).then((response) => new Blob([response.data]));
}