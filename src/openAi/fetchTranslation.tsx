import axios, { RawAxiosRequestHeaders } from "axios";
import { OPEN_AI_API_ENDPOINT } from "../config";

interface GetTranslationConfig {
  apiKey: string;
  text: string;
  targetLanguage: string;
  model?: string;
  temperature?: number;
}

interface ChatCompletionResponse {
  choices: Array<{
    message: {
      content: string;
    };
  }>;
}

export async function fetchTranslation({ apiKey, text, targetLanguage, model = "gpt-3.5-turbo", temperature = 0.3 }: GetTranslationConfig) {
  const prompt = `Translate the following text to ${targetLanguage}:\n\n${text}`;

  const body = {
    model,
    messages: [
      {
        role: "system",
        content:
          "You are a helpful translator. Translate the user's text accurately while preserving the original meaning and tone. If you are unable to translate the text for any reason, reply with exactly '???'.",
      },
      {
        role: "user",
        content: prompt,
      },
    ],
    temperature,
    max_tokens: 1000,
  };

  const headers: RawAxiosRequestHeaders = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${apiKey}`,
  };

  const response = await axios.post<ChatCompletionResponse>(new URL("/v1/chat/completions", OPEN_AI_API_ENDPOINT).toString(), body, {
    headers,
  });

  return response.data.choices[0].message.content.trim();
}
