// Звернення до системи лише через nginx: адреса, X-User-Id (до ЛР3), Problem Details.
// Клієнт працює поза Compose:  python -m http.server 8081 --bind 127.0.0.1 --directory web/client

export const API = localStorage.getItem("api") ?? "http://localhost:8080";

export class Problem extends Error {
    constructor(status, body) {
        super(body?.title ?? `HTTP ${status}`);
        this.status = status;
        this.body = body;
    }
}

let userId = sessionStorage.getItem("userId") ?? "u-driver-1";

export const getUser = () => userId;
export function setUser(value) {
    userId = value;
    sessionStorage.setItem("userId", value);
}

export async function call(method, path, {body, headers = {}} = {}) {
    const reply = await fetch(API + path, {
        method,
        headers: {
            ...(body !== undefined ? {"Content-Type": "application/json"} : {}),
            "X-User-Id": userId,
            ...headers,
        },
        body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    const type = reply.headers.get("content-type") ?? "";
    const data = type.includes("json") ? await reply.json() : await reply.text();
    if (!reply.ok) {
        throw new Problem(reply.status, typeof data === "object" ? data : {title: data});
    }
    return {status: reply.status, headers: reply.headers, data};
}

// Гроші приходять цілими копійками; показуємо гривні без float-арифметики над сумою.
export function money(minor, currency = "UAH") {
    const sign = minor < 0 ? "-" : "";
    const abs = Math.abs(minor);
    return `${sign}${Math.trunc(abs / 100)}.${String(abs % 100).padStart(2, "0")} ${currency}`;
}

// Моменти – у зоні області (Київ), незалежно від зони браузера.
export const kyivTime = (iso) =>
    new Date(iso).toLocaleString("uk-UA", {timeZone: "Europe/Kyiv", dateStyle: "short", timeStyle: "medium"});
