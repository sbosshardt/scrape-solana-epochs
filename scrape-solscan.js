const fs = require('fs')
const {parse} = require('csv-parse')
const {stringify} = require('csv-stringify/sync')
const playwright = require('playwright')
const cheerio = require('cheerio')
// TODO: support specifying the filename for epochs?
const filename = 'epochs.csv'
var epochList = {}
var epochFBUTimestampIndex = {}
var epochLBUTimestampIndex = {}

function unixTime(timestampString) {
  // Replace " at " with " " to make it parseable by Date constructor.
  const formattedInputDate = timestampString.replace(' at ', ' ')
  // Parse the date string
  const date = new Date(formattedInputDate)
  // getTime() is milliseconds since the Unix Epoch but we want seconds.
  const unix_timestamp = date.getTime() / 1000
  return unix_timestamp
}

// Selector prefix. We are selecting using the nth row of the table.
function sp(trNum) {
  return 'table.card-table tbody.list tr:eq(' + trNum + ') .font-monospace '
}

function sleep(time) {
  return new Promise((resolve) => setTimeout(resolve, time))
}

function addEpochToList(epoch) {
  epochList[epoch.epoch_num] = epoch
  epochFBUTimestampIndex[epoch.first_block_unix_timestamp] = epoch.epoch_num
  epochLBUTimestampIndex[epoch.last_block_unix_timestamp] = epoch.epoch_num
}

function writeEpochsCsv() {
  // {123: {epoch_num: 123, ...}, 124: {epoch_num: 124, ...}}
  const sortedEpochList = Object.keys(epochList)
    .sort()
    .reduce((accumulator, key) => {
      accumulator[key] = epochList[key]
      return accumulator
    }, {})
  const epochs = Object.values(sortedEpochList)
  const output = stringify(epochs, {
    header: true,
  })
  fs.writeFileSync(filename, output)
}

function readEpochsCsv() {
  if (!fs.existsSync(filename)) {
    console.log('File '+filename+' does not yet exist.')
    return
  }
  const data = fs.readFileSync(filename)
  const records = parse(data, { columns: true, skip_empty_lines: true })
  records.forEach(function (epoch) {
    addEpochToList(epoch)
  })
}

// e.g. sleep for 1-5 seconds: await randomDelay(1, 5)
async function randomDelay(minSecs, maxSecs) {
  const deltaSecs = maxSecs - minSecs
  const delay = Math.round(1000 * (minSecs + Math.random() * deltaSecs))
  console.log('Sleeping for', delay / 1000, 'seconds.')
  await sleep(delay)
}

async function main() {
  const browser = await playwright.chromium.launch({ headless: true })
  //const placeholderPage = await browser.newPage()
  //await placeholderPage.goto('chrome://version')

  async function scrapeEpochPage(epochNum) {
    const url = 'https://explorer.solana.com/epoch/' + epochNum
    const page = await browser.newPage()
    await page.goto(url, { waitUntil: 'networkidle' })
    let html = await page.content()

    const $ = cheerio.load(html)
    let obj = {}
    let arr = []
    obj['epoch_num'] = $(sp(0) + 'span').text()
    obj['previous_epoch_num'] = $(sp(1) + 'a').text()
    obj['next_epoch_num'] = $(sp(2) + 'a').text()
    obj['first_slot'] = $(sp(3) + 'span').text()
    obj['last_slot'] = $(sp(4) + 'span').text()
    obj['first_block_timestamp'] = $(sp(5)).text()
    obj['first_block_unix_timestamp'] = unixTime(obj['first_block_timestamp'])
    obj['first_block'] = $(sp(6) + 'a').text()
    obj['last_block'] = $(sp(7) + 'a').text()
    obj['last_block_timestamp'] = $(sp(8)).text()
    obj['last_block_unix_timestamp'] = unixTime(obj['last_block_timestamp'])

    await page.close()
    return obj
  }

  async function testEpoch(num) {
    let epoch = await scrapeEpochPage(num)
    console.log('Returned epoch:', epoch)
  }
  //await testEpoch(416)
  //await testEpoch(417)

  async function testRange() {
    // need 392-553 for the year 2023.
    //for (let epNum = 392; epNum < 553; epNum++) {
    readEpochsCsv()
    let delayNeeded = false
    for (let epNum = 260; epNum < 300; epNum++) {
      if (typeof(epochList[epNum]) !== 'undefined') {
        console.log('Epoch '+epNum+' is already known. Skipping.')
        continue
      }
      if (delayNeeded) {
        await randomDelay(1, 5)
      }
      console.log('Calling scrapeEpochPage() for epoch:', epNum)
      const epoch = await scrapeEpochPage(epNum)
      console.log('Scraped epoch:', epoch)
      epochList[epoch.epoch_num] = epoch
      delayNeeded = true
      if (typeof(epoch.next_epoch_num) === 'undefined' || !epoch.next_epoch_num) {
        console.log('Processed the latest epoch that exists presently.')
        break;
      }
    }
    console.log('Finished retrieving. Updating csv file.')
    writeEpochsCsv()
  }
  await testRange()

  await browser.close()
}
main()
