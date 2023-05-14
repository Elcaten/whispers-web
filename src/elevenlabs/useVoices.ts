import useSwrImmutable from "swr/immutable";
import { client } from "./client";

export function useVoices() {
  return useSwrImmutable("/v1/voices", url => client.get<VoicesResponse>(url).then(response => response.data))
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
