// Звернення до системи лише через nginx: адреса, cookie сесії (ЛР3), Problem Details.
// Клієнт працює поза Compose:  python -m http.server 8081 --bind 127.0.0.1 --directory web/client
// і відкривається як http://localhost:8081 (або http://127.0.0.1:8081).
//
// ЛР3, О3.3 – де живе «токен». Сесія – cookie session з HttpOnly: JavaScript (і чужий скрипт
// при XSS) його не читає й не зберігає в localStorage; браузер сам додає cookie до запитів з
// credentials: "include". Адреса API береться з того самого хоста, що й сторінка: localhost:8081 →
// localhost:8080, тоді клієнт і API – один «сайт», і cookie SameSite=Lax надсилається.

export const API = globalThis.localStorage?.getItem("api") ?? `http://${globalThis.location?.hostname ?? "localhost"}:8080`;

export class Problem extends Error {
    constructor(status, body) {
        super(body?.title ?? `HTTP ${status}`);
        this.status = status;
        this.body = body;
    }
}

export async function call(method, path, {body, headers = {}} = {}) {
    const reply = await fetch(API + path, {
        method,
        headers: {
            ...(body !== undefined ? {"Content-Type": "application/json"} : {}),
            ...headers,
        },
        body: body !== undefined ? JSON.stringify(body) : undefined,
        credentials: "include",
    });

    const type = reply.headers.get("content-type") ?? "";
    const data = type.includes("json") ? await reply.json() : await reply.text();
    if (!reply.ok) {
        throw new Problem(reply.status, typeof data === "object" ? data : {title: data});
    }
    return {status: reply.status, headers: reply.headers, data};
}

// ---------------------------------------------------------------- ЛР2 Web, О2.4: повтори
// Змінювальний запит повторюється з ТИМ САМИМ Idempotency-Key у всіх спробах, тож сервер виконає
// його не більше одного разу. Пауза – експоненційна з повним розсіюванням (full jitter):
// random(0, min(cap, base·2^(спроба−1))), але не менша за Retry-After. Обмежено і кількість
// спроб, і загальний час очікування. Якщо відповіді так і не було – «результат невідомий».

export const RETRY = {attempts: 5, baseMs: 400, capMs: 4000, budgetMs: 15000, timeoutMs: 8000};

// повторюємо лише те, що може змінитися з часом: обрив/тайм-аут, 5xx, 429, «ключ ще обробляється»
const retryable = (status, body) =>
    status === 429 || status >= 500 || (status === 409 && String(body?.type ?? "").endsWith("idempotency-key-in-progress"));

export class UnknownResult extends Error {
    constructor(key, attempts, last) {
        super("Результат невідомий");
        Object.assign(this, {key, attempts, last});
    }
}

export const newKey = () => `k-${crypto.randomUUID()}`;

export async function mutate(method, path, body, {
    policy = RETRY, key = newKey(), fetchFn = fetch, sleep = (ms) => new Promise((r) => setTimeout(r, ms)),
    random = Math.random, onAttempt = () => {},
} = {}) {
    const started = Date.now();
    let last = null;
    for (let attempt = 1; attempt <= policy.attempts; attempt++) {
        let retryAfterMs = 0;
        try {
            const reply = await fetchFn(API + path, {
                method,
                headers: {"Content-Type": "application/json", "Idempotency-Key": key},
                body: JSON.stringify(body),
                credentials: "include",
                signal: AbortSignal.timeout(policy.timeoutMs),
            });
            const type = reply.headers.get("content-type") ?? "";
            const data = type.includes("json") ? await reply.json() : await reply.text();
            onAttempt({attempt, status: reply.status, key});
            if (!retryable(reply.status, data)) {
                if (!reply.ok) throw new Problem(reply.status, typeof data === "object" ? data : {title: data});
                return {status: reply.status, headers: reply.headers, data, attempts: attempt, key};
            }
            last = `HTTP ${reply.status}`;
            retryAfterMs = 1000 * (Number(reply.headers.get("retry-after")) || 0);
        } catch (error) {
            if (error instanceof Problem) throw error;  // остаточна відповідь сервера
            last = error.name ?? "network error";        // обрив, тайм-аут: чи виконано – невідомо
            onAttempt({attempt, status: null, error: last, key});
        }
        if (attempt === policy.attempts) break;
        const backoff = Math.min(policy.capMs, policy.baseMs * 2 ** (attempt - 1));
        const delay = Math.max(retryAfterMs, Math.round(random() * backoff));
        if (Date.now() - started + delay > policy.budgetMs) break;
        await sleep(delay);
    }
    throw new UnknownResult(key, policy.attempts, last);
}

// ---------------------------------------------------------------- ЛР3: вхід і вихід

export const login = (login, password) => call("POST", "/v1/auth/login", {body: {login, password}});
export const logout = () => call("POST", "/v1/auth/logout");
export async function me() {
    try {
        return (await call("GET", "/v1/me")).data;
    } catch (error) {
        if (error instanceof Problem && error.status === 401) return null;  // не увійшов або сесія скінчилась
        throw error;
    }
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
