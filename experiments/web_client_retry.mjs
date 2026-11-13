// Web О2.4: перевірка політики повторів клієнта (web/client/api.js) без браузера й без сервера.
//   node experiments/web_client_retry.mjs
// Підставляємо fetch, що віддає заздалегідь задані відповіді, і sleep, що лише записує паузи.
import {mutate, RETRY, UnknownResult} from "../web/client/api.js";

let failures = 0;
const check = (name, ok, info = "") => {
    failures += ok ? 0 : 1;
    console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : "  " + JSON.stringify(info)}`);
};
const reply = (status, body = {}, headers = {}) => new Response(JSON.stringify(body), {
    status, headers: {"content-type": "application/json", ...headers}});

function fake(script) {
    const seen = {keys: [], sleeps: []};
    let i = 0;
    return {
        seen,
        fetchFn: async (_url, init) => {
            seen.keys.push(init.headers["Idempotency-Key"]);
            const step = script[Math.min(i++, script.length - 1)];
            if (step === "drop") throw new TypeError("fetch failed");
            return step();
        },
        sleep: async (ms) => { seen.sleeps.push(ms); },
    };
}

// 1. обрив, 503 з Retry-After: 2, «ключ ще обробляється», потім 201
{
    const f = fake(["drop", () => reply(503, {}, {"retry-after": "2"}),
        () => reply(409, {type: "x/idempotency-key-in-progress"}, {"retry-after": "1"}), () => reply(201, {id: "r1"})]);
    const r = await mutate("POST", "/v1/rentals", {car_id: "c"}, {fetchFn: f.fetchFn, sleep: f.sleep, random: () => 0.5});
    check("успіх на 4-й спробі", r.status === 201 && r.attempts === 4, r);
    check("той самий Idempotency-Key у всіх спробах", new Set(f.seen.keys).size === 1 && f.seen.keys.length === 4, f.seen.keys);
    check("пауза після 503 не менша за Retry-After (2000 мс)", f.seen.sleeps[1] >= 2000, f.seen.sleeps);
    check("пауза після 409 in-progress не менша за Retry-After (1000 мс)", f.seen.sleeps[2] >= 1000, f.seen.sleeps);
    check("пауза після обриву – розсіяна частка base·2^0", f.seen.sleeps[0] === Math.round(0.5 * RETRY.baseMs), f.seen.sleeps);
}

// 2. сервер мовчить: не більше attempts спроб, далі «результат невідомий»
{
    const f = fake(["drop"]);
    let error = null;
    try { await mutate("POST", "/v1/rentals", {car_id: "c"}, {fetchFn: f.fetchFn, sleep: f.sleep, random: () => 1}); }
    catch (e) { error = e; }
    check("після вичерпання спроб – UnknownResult («результат невідомий»)", error instanceof UnknownResult, String(error));
    check(`спроб рівно ${RETRY.attempts}`, f.seen.keys.length === RETRY.attempts, f.seen.keys.length);
    check("паузи ростуть експоненційно до межі cap", JSON.stringify(f.seen.sleeps) === JSON.stringify([400, 800, 1600, 3200]), f.seen.sleeps);
}

// 3. загальний бюджет очікування: великий Retry-After зупиняє повтори раніше
{
    const f = fake([() => reply(429, {}, {"retry-after": "20"})]);
    let error = null;
    try { await mutate("POST", "/v1/rentals", {car_id: "c"}, {fetchFn: f.fetchFn, sleep: f.sleep}); }
    catch (e) { error = e; }
    check("Retry-After 20 с > бюджету 15 с – одразу «результат невідомий», без очікування",
        error instanceof UnknownResult && f.seen.sleeps.length === 0, f.seen);
}

// 4. остаточна відповідь (409 car-busy, 422) не повторюється
{
    const f = fake([() => reply(409, {type: "x/car-busy", title: "Car is already rented"})]);
    let error = null;
    try { await mutate("POST", "/v1/rentals", {car_id: "c"}, {fetchFn: f.fetchFn, sleep: f.sleep}); }
    catch (e) { error = e; }
    check("409 car-busy – без повторів, Problem клієнту", error?.status === 409 && f.seen.keys.length === 1, f.seen);
}

console.log(failures ? `\n${failures} FAIL` : "\nOK");
process.exit(failures ? 1 : 0);
