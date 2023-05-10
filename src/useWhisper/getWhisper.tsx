import axios, { RawAxiosRequestHeaders } from "axios";

interface GetWhisperConfig {
  language?: "en" | "es";
}

const defaultWhisperConfig: Required<GetWhisperConfig> = {
  language: "en",
};

export async function getWhisper(file: File, config?: GetWhisperConfig) {
  console.log("getWhisper");

  const body = new FormData();
  body.append("file", file);
  body.append("model", "whisper-1");
  body.append("translate", "true");
  body.append("language", config?.language ?? defaultWhisperConfig.language);

  const headers: RawAxiosRequestHeaders = {};
  headers["Content-Type"] = "multipart/form-data";
  headers[
    "Authorization"
  ] = `Bearer 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`;

  const whisperApiEndpoint = "https://api.openai.com/v1/audio/";

  const response = await axios.post(
    whisperApiEndpoint + "transcriptions",
    body,
    {
      headers,
    }
  );

  return response.data.text as string;
}
