/* eslint-disable */
import React from "react"
import styles from "../css/contact.module.css"
import Web3 from "web3";

class TxHistory extends React.Component {
  constructor(props) {
    super(props);
    this.state = {}
    this.stateWeb3 = new Web3("wss://c8ad6024931d4eb293e999298b49d110.eth.ws.stage.rivet.cloud")
    this.logWeb3 = new Web3("https://d2a908wftkmhv6.cloudfront.net/")
  }
  componentDidUpdate() {
    console.log(this.props.address)
    if(!this.props.address) { return }
    if(this.props.address == this.state.address) { return }
    this.setState({address: this.props.address});
    let addressPromise;
    if (this.props.address.endsWith(".eth")) {
      addressPromise = this.stateWeb3.eth.ens.getAddress(this.props.address)
    } else {
      if (!this.stateWeb3.utils.isAddress(this.props.address)) {
        return
      }
      addressPromise = Promise.resolve(this.props.address)
    }
    addressPromise.then((address) => {
      console.log(address)
      return Promise.all([
        this.logWeb3.eth.getPastLogs({fromBlock: 0, toBlock: "latest", topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", null, "0x000000000000000000000000" + address.slice(2)]}),
        this.logWeb3.eth.getPastLogs({fromBlock: 0, toBlock: "latest", topics: ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", "0x000000000000000000000000" + address.slice(2)]})
      ])
    }).then((results) => {
      console.log(results)
      let txMapping = {}
      for(let tx of results[0]) {
        if(!txMapping[tx.address]) {
          txMapping[tx.address] = []
        }
        txMapping[tx.address].push({
          type: "received",
          from: this.props.address,
          to: "0x" + tx.topics[1].slice(-40),
          value: parseInt(tx.data),
          key: tx.blockHash + tx.logIndex,
          bn: tx.blockNumber,
        });
        txMapping[tx.address].sort((a, b) => a.bn < b.bn ? 1 : -1)
      }
      for(let tx of results[1]) {
        if(!txMapping[tx.address]) {
          txMapping[tx.address] = []
        }
        txMapping[tx.address].push({
          type: "sent",
          from: "0x" + tx.topics[1].slice(-40),
          to: this.props.address,
          value: parseInt(tx.data),
          key: tx.blockHash + tx.logIndex,
          bn: tx.blockNumber,
        });
        txMapping[tx.address].sort((a, b) => a.bn < b.bn ? 1 : -1)
      }
      this.setState({txMapping: txMapping});
    }).catch(console.log)

  }
  setAddress(e) {
    this.setState({address: e.target.value})
  }
  render() {
    if(!this.state.txMapping) { return null }
    let tokens = [];
    for(let address in this.state.txMapping) {
      let entries = [];
      for(let entry of this.state.txMapping[address]) {
        console.log(entry)
        entries.push(
          <li key={entry.key} className={styles.historyItem}>
            {entry.type} - {entry.value} - {entry.from} - {entry.to}
          </li>
        )
      }
      console.log(entries, address, );
      tokens.push(
        <li key={address}>
          <details className={styles.result}>
            <summary>{address}</summary>
            <ul className={styles.resultExpanded}><li className={styles.historyHeader}>sent/recieved — amount — from/to account/ens — block number — date/time (reverse chron.)</li>
              {entries}
            </ul>
          </details>
        </li>
      )
    }

    return (
      <ul className={styles.listBox}>
        <h1 className={styles.resultHeader}>Address (Expand for Transaction History)</h1>
        {tokens}
      </ul>
    )
  }
}

export default TxHistory
