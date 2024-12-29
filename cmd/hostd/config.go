package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) string {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	checkFatalError("failed to read password input", err)
	fmt.Println("")
	return string(input)
}

func readInput(context string) string {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	checkFatalError("failed to read input", err)
	return strings.TrimSpace(input)
}

// wrapANSI wraps the output in ANSI escape codes if enabled.
func wrapANSI(prefix, output, suffix string) string {
	if cfg.Log.StdOut.EnableANSI {
		return prefix + output + suffix
	}
	return output
}

func humanList(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	} else if len(s) == 1 {
		return fmt.Sprintf(`%q`, s[0])
	} else if len(s) == 2 {
		return fmt.Sprintf(`%q %s %q`, s[0], sep, s[1])
	}

	var sb strings.Builder
	for i, v := range s {
		if i != 0 {
			sb.WriteString(", ")
		}
		if i == len(s)-1 {
			sb.WriteString("or ")
		}
		sb.WriteString(`"`)
		sb.WriteString(v)
		sb.WriteString(`"`)
	}
	return sb.String()
}

func promptQuestion(question string, answers []string) string {
	for {
		input := readInput(fmt.Sprintf("%s (%s)", question, strings.Join(answers, "/")))
		for _, answer := range answers {
			if strings.EqualFold(input, answer) {
				return answer
			}
		}
		fmt.Println(wrapANSI("\033[31m", fmt.Sprintf("Answer must be %s", humanList(answers, "or")), "\033[0m"))
	}
}

func promptYesNo(question string) bool {
	answer := promptQuestion(question, []string{"yes", "no"})
	return strings.EqualFold(answer, "yes")
}

// stdoutError prints an error message to stdout
func stdoutError(msg string) {
	if cfg.Log.StdOut.EnableANSI {
		fmt.Println(wrapANSI("\033[31m", msg, "\033[0m"))
	} else {
		fmt.Println(msg)
	}
}

// mustSetAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func setAPIPassword() {
	// retry until a valid API password is entered
	for {
		fmt.Println("Please choose a password to unlock hostd.")
		fmt.Println("This password will be required to access the admin UI in your web browser.")
		fmt.Println("(The password must be at least 4 characters.)")
		cfg.HTTP.Password = readPasswordInput("Enter password")
		if len(cfg.HTTP.Password) >= 4 {
			break
		}

		fmt.Println(wrapANSI("\033[31m", "Password must be at least 4 characters!", "\033[0m"))
		fmt.Println("")
	}
}

func setSeedPhrase() {
	// retry until a valid seed phrase is entered
	for {
		fmt.Println("")
		fmt.Println("Type in your 12-word seed phrase and press enter. If you do not have a seed phrase yet, type 'seed' to generate one.")
		phrase := readPasswordInput("Enter seed phrase")

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
			fmt.Println("A new seed phrase has been generated below. " + wrapANSI("\033[1m", "Write it down and keep it safe.", "\033[0m"))
			fmt.Println("Your seed phrase is the only way to recover your Siacoin. If you lose your seed phrase, you will also lose your Siacoin.")
			fmt.Println("You will need to re-enter this seed phrase every time you start hostd.")
			fmt.Println("")
			fmt.Println(wrapANSI("\033[34;1m", "Seed Phrase:", "\033[0m"), phrase)
			fmt.Println(wrapANSI("\033[34;1m", "Wallet Address:", "\033[0m"), types.StandardUnlockHash(key.PublicKey()))

			// confirm seed phrase
			for {
				fmt.Println("")
				fmt.Println(wrapANSI("\033[1m", "Please confirm your seed phrase to continue.", "\033[0m"))
				confirmPhrase := readPasswordInput("Enter seed phrase")
				if confirmPhrase == phrase {
					cfg.RecoveryPhrase = phrase
					return
				}

				fmt.Println(wrapANSI("\033[31m", "Seed phrases do not match!", "\033[0m"))
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			fmt.Println(wrapANSI("\033[31m", "Invalid seed phrase:", "\033[0m"), err)
			fmt.Println("You entered:", phrase)
			continue
		}

		// valid seed phrase
		cfg.RecoveryPhrase = phrase
		break
	}
}

func setListenAddress(context string, value *string) {
	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if input == "" {
			return
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid %s port %q: must be between 0 and 65535", context, input))
			continue
		}
		*value = net.JoinHostPort(host, port)
		return
	}
}

func setAdvancedConfig() {
	if !promptYesNo("Would you like to configure advanced settings?") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure the host's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// http address
	fmt.Println("The HTTP address is used to serve the host's admin API.")
	fmt.Println("The admin API is used to configure the host.")
	fmt.Println("It should not be exposed to the public internet without setting up a reverse proxy.")
	setListenAddress("HTTP Address", &cfg.HTTP.Address)

	// syncer address
	fmt.Println("")
	fmt.Println("The syncer address is used to exchange blocks with other nodes in the Sia network")
	fmt.Println("It should be exposed publicly to improve the host's connectivity.")
	setListenAddress("Gateway Address", &cfg.Syncer.Address)

	// rhp2 address
	fmt.Println("")
	fmt.Println("The RHP2 address is used by renters to connect to the host.")
	fmt.Println("It is a legacy protocol, but still required for host discovery")
	fmt.Println("It should be exposed publicly to allow renters to connect and upload data.")
	setListenAddress("RHP2 Address", &cfg.RHP2.Address)

	// rhp3 TCP address
	fmt.Println("")
	fmt.Println("The RHP3 address is used by renters to connect to the host.")
	fmt.Println("It is a newer protocol that is more efficient than RHP2.")
	fmt.Println("It should be exposed publicly to allow renters to connect and upload data.")
	setListenAddress("RHP3 TCP Address", &cfg.RHP3.TCPAddress)
}

func setDataDirectory() {
	if cfg.Directory == "" {
		cfg.Directory = "."
	}

	dir, err := filepath.Abs(cfg.Directory)
	checkFatalError("failed to get absolute path of data directory", err)

	fmt.Println("The data directory is where hostd will store its metadata and consensus data.")
	fmt.Println("This directory should be on a fast, reliable storage device, preferably an SSD.")
	fmt.Println("")

	_, existsErr := os.Stat(filepath.Join(cfg.Directory, "hostd.db"))
	dataExists := existsErr == nil
	if dataExists {
		fmt.Println(wrapANSI("\033[33m", "There is existing data in the data directory.", "\033[0m"))
		fmt.Println(wrapANSI("\033[33m", "If you change your data directory, you will need to manually move consensus, gateway, tpool, and hostd.db to the new directory.", "\033[0m"))
	}

	if !promptYesNo("Would you like to change the data directory? (Current: " + dir + ")") {
		return
	}
	cfg.Directory = readInput("Enter data directory")
}

func configPath() string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return str
	}

	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "hostd", "hostd.yml")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "hostd", "hostd.yml")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "etc", "hostd", "hostd.yml")
	default:
		return "hostd.yml"
	}
}

func runConfigCmd(fp string) {
	fmt.Println("hostd Configuration Wizard")
	fmt.Println("This wizard will help you configure hostd for the first time.")
	fmt.Println("You can always change these settings with the config command or by editing the config file.")

	if fp == "" {
		fp = configPath()
	}
	fp, err := filepath.Abs(fp)
	checkFatalError("failed to get absolute path of config file", err)

	fmt.Println("")
	fmt.Printf("Config Location %q\n", fp)

	if _, err := os.Stat(fp); err == nil {
		if !promptYesNo(fmt.Sprintf("%q already exists. Would you like to overwrite it?", fp)) {
			return
		}
	}

	fmt.Println("")
	setDataDirectory()

	fmt.Println("")
	if cfg.RecoveryPhrase != "" {
		fmt.Println(wrapANSI("\033[33m", "A wallet seed phrase is already set.", "\033[0m"))
		fmt.Println("If you change your wallet seed phrase, your host will not be able to access Siacoin associated with this wallet.")
		fmt.Println("Ensure that you have backed up your wallet seed phrase before continuing.")
		if promptYesNo("Would you like to change your wallet seed phrase?") {
			setSeedPhrase()
		}
	} else {
		setSeedPhrase()
	}

	fmt.Println("")
	if cfg.HTTP.Password != "" {
		fmt.Println(wrapANSI("\033[33m", "An admin password is already set.", "\033[0m"))
		fmt.Println("If you change your admin password, you will need to update any scripts or applications that use the admin API.")
		if promptYesNo("Would you like to change your admin password?") {
			setAPIPassword()
		}
	} else {
		setAPIPassword()
	}

	setAdvancedConfig()

	// write the config file
	f, err := os.Create(fp)
	checkFatalError("failed to create config file", err)
	defer f.Close()

	enc := yaml.NewEncoder(f)
	defer enc.Close()

	checkFatalError("failed to encode config file", enc.Encode(cfg))
	checkFatalError("failed to sync config file", f.Sync())
}
