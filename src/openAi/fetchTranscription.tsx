import axios, { RawAxiosRequestHeaders } from "axios";
import { OPEN_AI_API_ENDPOINT } from "../config";

type ResonseFormat = "json" | "text" | "verbose_json";

interface GetTranscriptionsConfig<TResponseFormat extends ResonseFormat> {
  apiKey: string;
  file: Blob;
  prompt?: string;
  response_format?: TResponseFormat;
  temperature?: number;
}

export type TextResponse = {
  text: string;
};

export type JsonResponse = {
  text: string;
  usage: {
    type: string;
    seconds: number;
  };
};

export type VerboseJsonResponse = {
  task: string;
  language: string;
  duration: number;
  text: string;
  segments: {
    id: number;
    seek: number;
    start: number;
    end: number;
    text: string;
    tokens: number[];
    temperature: number;
    avg_logprob: number;
    compression_ratio: number;
    no_speech_prob: number;
  }[];
  usage: {
    type: string;
    seconds: number;
  };
};

export async function fetchTranscription(config: GetTranscriptionsConfig<"text">): Promise<TextResponse>;
export async function fetchTranscription(config: GetTranscriptionsConfig<"json">): Promise<JsonResponse>;
export async function fetchTranscription(config: GetTranscriptionsConfig<"verbose_json">): Promise<VerboseJsonResponse>;
export async function fetchTranscription({
  apiKey,
  file,
  prompt,
  response_format = "verbose_json",
  temperature,
}: GetTranscriptionsConfig<"text" | "json" | "verbose_json">): Promise<TextResponse | JsonResponse | VerboseJsonResponse> {
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
  headers["Authorization"] = `Bearer ${apiKey}`;

  const response = await axios.post<{ text: string }>(new URL("/v1/audio/transcriptions", OPEN_AI_API_ENDPOINT).toString(), body, {
    headers,
  });

  return response.data;
}
