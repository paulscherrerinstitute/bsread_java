package ch.psi.bsread.command;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.commands.ReconnectCommand;
import ch.psi.bsread.message.commands.StopCommand;

@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "htype")
@JsonSubTypes({
		@Type(value = MainHeader.class, name = MainHeader.DEFAULT_HTYPE),
		@Type(value = ReconnectCommand.class, name = ReconnectCommand.DEFAULT_HTYPE),
		@Type(value = StopCommand.class, name = StopCommand.DEFAULT_HTYPE)
})
public abstract class AbstractCommand implements Command {
	private static final long serialVersionUID = 888923045544224695L;
}
