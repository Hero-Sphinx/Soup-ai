// Renders report/report.html to REPORT.pdf with the Chromium that the
// browser test already uses.   npx tsx scripts/build-report.ts
import { pathToFileURL } from "node:url";
import { chromium } from "playwright";

const browser = await chromium.launch();
const page = await browser.newPage();
await page.goto(pathToFileURL("report/report.html").href, { waitUntil: "load" });
const broken = await page.$$eval("img", (imgs) => imgs.filter((i) => !(i as HTMLImageElement).naturalWidth).map((i) => i.getAttribute("src")));
if (broken.length) throw new Error(`missing images: ${broken.join(", ")}`);
await page.pdf({
  path: "REPORT.pdf",
  format: "A4",
  printBackground: true,
  displayHeaderFooter: true,
  headerTemplate: "<span></span>",
  footerTemplate:
    '<div style="font-size:8px;color:#666;width:100%;text-align:center;">Soup tracker report · page <span class="pageNumber"></span> of <span class="totalPages"></span></div>',
  margin: { top: "16mm", bottom: "18mm", left: "15mm", right: "15mm" },
});
await browser.close();
console.log("wrote REPORT.pdf");
