import { once } from "node:events";
import { describe, expect, it } from "vitest";
import { Emitter } from "../src/emitter.js";

class Probe extends Emitter {
  entries(event: string) {
    return this.rawEntries(event);
  }
  drop(event: string, entry: ReturnType<Probe["entries"]>[number]) {
    this.dropEntry(event, entry);
  }
}

describe("Emitter", () => {
  it("calls listeners in registration order with the emitted args", () => {
    const e = new Emitter();
    const calls: string[] = [];
    e.on("x", (a: number) => calls.push(`a${a}`));
    e.addListener("x", (a: number) => calls.push(`b${a}`));
    expect(e.emit("x", 1)).toBe(true);
    expect(calls).toEqual(["a1", "b1"]);
  });

  it("once() fires once and is removed before it runs", () => {
    const e = new Emitter();
    let seenCount = -1;
    e.once("x", () => {
      seenCount = e.listenerCount("x");
    });
    e.emit("x");
    e.emit("x");
    expect(seenCount).toBe(0);
    expect(e.listenerCount("x")).toBe(0);
  });

  it("off()/removeListener() remove one registration; removeAllListeners() clears", () => {
    const e = new Emitter();
    const fn = () => {};
    e.on("x", fn);
    e.on("x", fn);
    e.off("x", fn);
    expect(e.listenerCount("x")).toBe(1);
    e.removeListener("x", fn);
    expect(e.listenerCount("x")).toBe(0);
    e.on("x", fn).on("y", fn);
    e.removeAllListeners("x");
    expect(e.listenerCount("y")).toBe(1);
    e.removeAllListeners();
    expect(e.listenerCount("y")).toBe(0);
  });

  it("emit iterates a snapshot: a listener added during emit runs next time only", () => {
    const e = new Emitter();
    const calls: string[] = [];
    e.on("x", () => {
      calls.push("first");
      e.on("x", () => calls.push("late"));
    });
    e.emit("x");
    expect(calls).toEqual(["first"]);
  });

  it("emitting 'error' with no listener does not throw", () => {
    const e = new Emitter();
    expect(e.emit("error", new Error("boom"))).toBe(false);
  });

  it("listeners() and rawEntries() report what is registered; dropEntry() removes exactly one entry", () => {
    const e = new Probe();
    const a = () => {};
    const b = () => {};
    e.on("x", a);
    e.once("x", b);
    expect(e.listeners("x")).toEqual([a, b]);
    const entries = e.entries("x");
    expect(entries.map((x) => x.once)).toEqual([false, true]);
    e.drop("x", entries[1]!);
    expect(e.listeners("x")).toEqual([a]);
  });

  it("works with node:events once() and leaves no listener behind", async () => {
    const e = new Emitter();
    const p = once(e as never, "ready");
    expect(e.listenerCount("ready")).toBe(1);
    expect(e.listenerCount("error")).toBe(1);
    e.emit("ready", 42);
    expect(await p).toEqual([42]);
    expect(e.listenerCount("ready")).toBe(0);
    expect(e.listenerCount("error")).toBe(0);
  });
});
