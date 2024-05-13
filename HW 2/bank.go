package bank

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Bank represents a simple banking system
type Bank struct {
	bankLock *sync.Mutex
	accounts map[int]*Account
}

type Account struct {
	balance int
	lock    *sync.Mutex
}

// initializes a new bank
func BankInit() *Bank {
	b := Bank{&sync.Mutex{}, make(map[int]*Account)}
	return &b
}

func (b *Bank) notifyAccountHolder(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) notifySupportTeam(accountID int) {
	b.sendEmailTo(accountID)
}

func (b *Bank) logInsufficientBalanceEvent(accountID int) {
	DPrintf("Insufficient balance in account %d for withdrawal\n", accountID)
}

func (b *Bank) sendEmailTo(accountID int) {
	// Dummy function
	// hello
	// marker
	// eraser
}

// creates a new account with the given account ID
func (b *Bank) CreateAccount(accountID int) {
	// your code here

	// intialize an Account struct
	acc := Account{balance: 0, lock: &sync.Mutex{}}

	// use the lock for write, which is Lock(), since we're writing data into the map
	b.bankLock.Lock()
	// unlock the lock
	defer b.bankLock.Unlock()

	// to ensure that no duplicate accounts are created. Otherwise it will cause data race
	_, exists := b.accounts[accountID]

	if exists {
		panic(fmt.Sprintf("Attempt to create a duplicate account with ID %d", accountID))
	}

	// write the account into the map
	b.accounts[accountID] = &acc
}

// deposit a given amount to the specified account
func (b *Bank) Deposit(accountID, amount int) {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][DEPOSIT] for account %d\n", accountID)

	newBalance := account.balance + amount
	account.balance = newBalance
	// b.accounts[accountID] = account
	DPrintf("Deposited %d into account %d. New balance: %d\n", amount, accountID, newBalance)

	// changed "b.accounts[accountID]" to "account", since reading might cause data race
	account.lock.Unlock()
	DPrintf("[RELEASED LOCK][DEPOSIT] for account %d\n", accountID)
}

// withdraw the given amount from the given account id
func (b *Bank) Withdraw(accountID, amount int) bool {
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	account.lock.Lock()
	DPrintf("[ACQUIRED LOCK][WITHDRAW] for account %d\n", accountID)

	if account.balance >= amount {

		newBalance := account.balance - amount
		account.balance = newBalance
		DPrintf("Withdrawn %d from account %d. New balance: %d\n", amount, accountID, newBalance)
		account.lock.Unlock()
		DPrintf("[RELEASED LOCK][WITHDRAW] for account %d\n", accountID)
		return true
	} else {
		// Insufficient balance in account %d for withdrawal
		// Please contact the account holder or take appropriate action.
		// trigger a notification or alert mechanism
		b.notifyAccountHolder(accountID)
		b.notifySupportTeam(accountID)
		// log the event for further investigation
		b.logInsufficientBalanceEvent(accountID)

		// else case also requires unlock, otherwise will cause a deadlock.
		account.lock.Unlock()
		return false
	}
}

func (b *Bank) Transfer(sender int, receiver int, amount int, allowOverdraw bool) bool {
	b.bankLock.Lock()
	senderAccount := b.accounts[sender]
	receiverAccount := b.accounts[receiver]
	b.bankLock.Unlock()

	success := false

	// to deal with deadlock:
	// use account id to determine the order of lock
	// lock sender first and then receiver
	if sender < receiver {
		senderAccount.lock.Lock()
		receiverAccount.lock.Lock()

		// if the sender has enough balance,
		// or if overdraws are allowed

		if senderAccount.balance >= amount || allowOverdraw {
			senderAccount.balance -= amount
			receiverAccount.balance += amount
			success = true
		}

		// unlock reveiver and then sender
		receiverAccount.lock.Unlock()
		senderAccount.lock.Unlock()

		// lock receiver first and then sender
	} else {
		receiverAccount.lock.Lock()
		senderAccount.lock.Lock()

		// if the sender has enough balance,
		// or if overdraws are allowed

		if senderAccount.balance >= amount || allowOverdraw {
			senderAccount.balance -= amount
			receiverAccount.balance += amount
			success = true
		}

		// lock receiver first and then sender
		senderAccount.lock.Unlock()
		receiverAccount.lock.Unlock()
	}

	return success
}

func (b *Bank) DepositAndCompare(accountId int, amount int, compareThreshold int) bool {

	// since the balance in the account might change soon after the function calls, cannot use funtion here
	var compareResult bool

	// do the deposit
	b.bankLock.Lock()
	account := b.accounts[accountId]
	b.bankLock.Unlock()

	account.lock.Lock()
	defer account.lock.Unlock()
	account.balance += amount

	// compare the balance and threshold
	compareResult = account.balance >= compareThreshold

	// return compared result
	return compareResult
}

// returns the balance of the given account id
func (b *Bank) GetBalance(accountID int) int {

	// need to lock the bank to prevent data race
	b.bankLock.Lock()
	account := b.accounts[accountID]
	b.bankLock.Unlock()

	// lock the account
	account.lock.Lock()
	defer account.lock.Unlock()
	DPrintf("[GetBalance] for account %d\n", accountID)
	return account.balance
}
