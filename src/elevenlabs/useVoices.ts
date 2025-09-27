import { useState, useEffect } from "react";
import { client } from "./client";

export function useVoices() {
  const [data, setData] = useState<VoicesResponse | undefined>(undefined);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    setIsLoading(true);
    setError(undefined);
    
    client.get<VoicesResponse>("/v1/voices")
      .then(response => response.data)
      .then(setData)
      .catch(setError)
      .finally(() => setIsLoading(false));
  }, []);

  return { data, error, isLoading };
}

interface VoicesResponse {
  voices: VoicesItem[];
}
interface VoicesItem {
  voice_id: string;
  name: string;
  samples: null;
  category: string;
  fine_tuning: Fine_tuning;
  labels: Labels;
  description: null;
  preview_url: string;
  available_for_tiers: any[];
  settings: null;
}
interface Fine_tuning {
  model_id: null;
  is_allowed_to_fine_tune: boolean;
  fine_tuning_requested: boolean;
  finetuning_state: string;
  verification_attempts: null;
  verification_failures: any[];
  verification_attempts_count: number;
  slice_ids: null;
}
interface Labels {
}
