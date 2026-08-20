import { RuntimeConsoleShell } from "../components/runtime-console-shell";

export default function RuntimeConsolePage() {
  return (
    <>
      <RuntimeConsoleShell />
      <script src="/static/app.js" defer />
    </>
  );
}