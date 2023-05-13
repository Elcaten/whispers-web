import axios, { RawAxiosRequestHeaders } from "axios";
import { OPEN_AI_API_ENDPOINT } from "./config";

interface GetTranslationConfig {
  apiKey: string
  file: File;
  prompt?: string;
  response_format?: "json" | "text" | "srt" | "verbose_json" | "vtt";
  temperature?: number;
}

export async function fetchTranslation(
  {
    apiKey,
    file,
    prompt,
    response_format = "json",
    temperature,
  }: GetTranslationConfig
) {
  const body = new FormData();
  body.append("file", file);
  body.append("model", "whisper-1");
  body.append("response_format", response_format);
  if (temperature) {
    body.append("temperature", temperature.toString(10));
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
    new URL("/v1/audio/translations", OPEN_AI_API_ENDPOINT).toString(),
    body,
    {
      headers,
    }
  );

  return response.data.text as string;
}

