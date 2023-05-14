import axios from "axios";
import { ELEVEN_LABS_API_KEY, ELEVEN_LABS_API_ENDPOINT } from "../config";

export const client = axios.create({ headers: { "xi-api-key": ELEVEN_LABS_API_KEY }, baseURL: ELEVEN_LABS_API_ENDPOINT })