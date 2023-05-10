import React from "react";
import "./App.css";
import { useWhisper } from "./useWhisper/useWhisper.ts";

export default function App() {
  const {
    recording,
    speaking,
    transcribing,
    transcript,
    pauseRecording,
    startRecording,
    stopRecording,
  } = useWhisper({
    apiKey: "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",
    // streaming: true,
    // removeSilence: true,
    // timeSlice: 1_000, // 1 second
  });

  return (
    <div>
      <p>Recording: {String(recording)}</p>
      <p>Speaking: {String(speaking)}</p>
      <p>Transcribing: {String(transcribing)}</p>
      <p>Transcribed Text: {transcript.text}</p>
      <button onClick={() => startRecording()}>Start</button>
      <button onClick={() => pauseRecording()}>Pause</button>
      <button onClick={() => stopRecording()}>Stop</button>
    </div>
  );
}
