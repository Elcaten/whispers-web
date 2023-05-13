import axios, { RawAxiosRequestHeaders } from "axios";
import { OPEN_AI_API_ENDPOINT } from "./config";

interface GetTranscriptionsConfig {
  apiKey: string
  file: File;
  language?: "en" | "es";
  prompt?: string;
  response_format?: "json" | "text" | "srt" | "verbose_json" | "vtt";
  temperature?: number;
}

export async function fetchTranscription({
  apiKey,
  file,
  language,
  prompt,
  response_format = "json",
  temperature,
}: GetTranscriptionsConfig): Promise<string> {
  const body = new FormData();
  body.append("file", file);
  body.append("model", "whisper-1");
  body.append("response_format", response_format);
  if (temperature) {
    body.append("temperature", temperature.toString(10));
  }
  if (language) {
    body.append("language", language);
  }
  if (prompt) {
    body.append("prompt", prompt);
  }

  const headers: RawAxiosRequestHeaders = {};
  headers["Content-Type"] = "multipart/form-data";
  headers[
    "Authorization"
  ] = `Bearer ${apiKey}`;

  const response = await axios.post<{ text: string }>(
    new URL("/v1/audio/transcriptions", OPEN_AI_API_ENDPOINT).toString(),
    body,
    {
      headers,
    }
  );

  return response.data.text as string;
}
