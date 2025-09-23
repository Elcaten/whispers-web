export const OPEN_AI_API_KEY = import.meta.env.VITE_OPENAI_API_KEY as string;
export const ELEVEN_LABS_API_KEY = import.meta.env.VITE_ELEVENLABS_API_KEY as string;

export const OPEN_AI_API_ENDPOINT = (import.meta.env.VITE_OPENAI_API_ENDPOINT ?? "https://api.openai.com/") as string;
export const ELEVEN_LABS_API_ENDPOINT = (import.meta.env.VITE_ELEVENLABS_API_ENDPOINT ?? "https://api.elevenlabs.io") as string;

export const ELEVEN_LABS_VOICE_ID = (import.meta.env.VITE_ELEVENLABS_VOICE_ID ?? "MF3mGyEYCl7XYWbV9V6O") as string;
